package chatgpt

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

// hostOS is runtime.GOOS; a package var so the macOS-only paths are testable
// on the Linux CI runner.
var hostOS = runtime.GOOS

// Setup is the browser-bootstrap flow: every side effect (install, open a
// URL, prompt) is injected so it is testable and so non-interactive callers
// can opt out.
type Setup struct {
	Host chromium.Host
	// Run executes a command (brew install) and returns its exit code.
	Run func(argv []string) int
	// Open opens an app bundle or URL (`open`).
	Open func(target string) error
	// Prompt asks the user to press Enter; nil means no prompting is possible.
	Prompt func(message string) error
	// Log writes progress to stderr.
	Log func(message string)
	// Discover captures a session from a browser key.
	Discover func(browserKey string) (CapturedSession, error)
	// BrewAvailable says whether Homebrew is on PATH.
	BrewAvailable func() bool
	// Sleep is between opening the app and the URL.
	Sleep func(time.Duration)
	// MaxAttempts bounds the sign-in retry loop (default 3).
	MaxAttempts int
}

// DefaultSetup wires the real machine.
func DefaultSetup(host chromium.Host, stdin io.Reader, stderr io.Writer) Setup {
	reader := bufio.NewReader(stdin)
	return Setup{
		Host: host,
		Run: func(argv []string) int {
			cmd := exec.Command(argv[0], argv[1:]...)
			cmd.Stdout, cmd.Stderr = stderr, stderr
			if err := cmd.Run(); err != nil {
				if exit, ok := err.(*exec.ExitError); ok {
					return exit.ExitCode()
				}
				return 1
			}
			return 0
		},
		Open: func(target string) error { return exec.Command("open", target).Run() },
		Prompt: func(message string) error {
			fmt.Fprint(stderr, message)
			_, err := reader.ReadString('\n')
			return err
		},
		Log:           func(message string) { fmt.Fprintln(stderr, message) },
		Discover:      func(key string) (CapturedSession, error) { return Discover(host, key) },
		BrewAvailable: func() bool { _, err := exec.LookPath("brew"); return err == nil },
		Sleep:         time.Sleep,
	}
}

// EnsureBrowser returns a Chrome-family browser to use, installing one if
// necessary. prefer forces a browser key; with autoInstall and nothing
// suitable installed, DefaultInstallBrowser is installed via Homebrew.
func (s Setup) EnsureBrowser(prefer string, autoInstall bool) (chromium.Profile, error) {
	if prefer != "" {
		profile, ok := chromium.BrowserByKey(prefer)
		if !ok {
			return chromium.Profile{}, cookieErrorf("unknown browser %q", prefer)
		}
		if profile.AppBundle != "" && s.installed(profile) {
			return profile, nil
		}
		if !autoInstall {
			return chromium.Profile{}, cookieErrorf("%s is not installed", profile.DisplayName)
		}
		if err := s.install(profile); err != nil {
			return chromium.Profile{}, err
		}
		return profile, nil
	}
	if installed := s.Host.Installed(); len(installed) > 0 {
		return installed[0], nil
	}
	if !autoInstall {
		return chromium.Profile{}, cookieErrorf("no Chrome-family browser is installed; install Chrome/Brave/Edge/Arc and log into chatgpt.com")
	}
	target, _ := chromium.BrowserByKey(chromium.DefaultInstallBrowser)
	if err := s.install(target); err != nil {
		return chromium.Profile{}, err
	}
	return target, nil
}

func (s Setup) installed(profile chromium.Profile) bool {
	return profile.AppBundle != "" && s.Host.FileExists != nil && s.Host.FileExists(profile.AppBundle)
}

func (s Setup) install(profile chromium.Profile) error {
	if hostOS != "darwin" {
		return cookieErrorf("cannot auto-install %s on this platform; install it manually", profile.DisplayName)
	}
	if profile.HomebrewCask == "" || s.BrewAvailable == nil || !s.BrewAvailable() {
		return cookieErrorf("Homebrew is required to auto-install %s; install it manually (https://brew.sh) or install the browser yourself", profile.DisplayName)
	}
	s.log(fmt.Sprintf("Installing %s via Homebrew (brew install --cask %s)...", profile.DisplayName, profile.HomebrewCask))
	if code := s.Run([]string{"brew", "install", "--cask", profile.HomebrewCask}); code != 0 {
		return cookieErrorf("`brew install --cask %s` failed (exit %d)", profile.HomebrewCask, code)
	}
	if !s.installed(profile) {
		return cookieErrorf("%s did not appear after install", profile.DisplayName)
	}
	s.log(fmt.Sprintf("Installed %s.", profile.DisplayName))
	return nil
}

func (s Setup) log(message string) {
	if s.Log != nil {
		s.Log(message)
	}
}

// EnsureLoggedIn returns a captured chatgpt.com session, guiding the user to
// sign in: if absent, it opens chatgpt.com in the browser and waits for the
// user to confirm, retrying up to MaxAttempts.
func (s Setup) EnsureLoggedIn(profile chromium.Profile) (CapturedSession, error) {
	attempts := s.MaxAttempts
	if attempts <= 0 {
		attempts = 3
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		captured, err := s.Discover(profile.Key)
		if err == nil {
			return captured, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		s.log(fmt.Sprintf("No logged-in chatgpt.com session in %s yet. Opening chatgpt.com; please sign in (attempt %d/%d).", profile.DisplayName, attempt, attempts-1))
		if profile.AppBundle != "" && s.Open != nil {
			_ = s.Open(profile.AppBundle)
			if s.Sleep != nil {
				s.Sleep(500 * time.Millisecond)
			}
		}
		if s.Open != nil {
			_ = s.Open("https://chatgpt.com/")
		}
		if s.Prompt != nil {
			if err := s.Prompt("Press Enter once you have signed in to chatgpt.com... "); err != nil {
				break
			}
		}
	}
	if lastErr == nil {
		lastErr = cookieErrorf("could not capture a chatgpt.com session")
	}
	return CapturedSession{}, lastErr
}
