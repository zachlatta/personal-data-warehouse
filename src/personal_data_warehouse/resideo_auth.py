"""Sign in to Resideo and install the credential the thermostat poller uses.

    uv run personal-data-warehouse-resideo-auth

It prints a URL to open in a browser, because the sign-in page can ask a person
for a CAPTCHA. Once signed in, the browser tries to open the app and fails:
paste the address of that failed redirect back here (with the browser's network
panel open if the address bar does not show it). The token goes to
private.resideo_oauth_tokens on the POSTGRES_DATABASE_URL in the environment.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json

from personal_data_warehouse.config import RESIDEO_ACCOUNT, load_settings
from personal_data_warehouse.resideo_api import (
    authorization_code_from_redirect,
    build_authorize_request,
    exchange_authorization_code,
)


def main() -> int:
    request = build_authorize_request()
    print("Open this URL, sign in, and paste the com.resideo.firstalert:// address it redirects to:\n")
    print(request.url + "\n")
    pasted = input("Redirect URL: ")
    code = authorization_code_from_redirect(pasted, expected_state=request.state)
    token = exchange_authorization_code(code, code_verifier=request.code_verifier)

    from personal_data_warehouse.warehouse import warehouse_from_settings

    warehouse = warehouse_from_settings(load_settings(require_gmail=False))
    try:
        warehouse.ensure_resideo_tables()
        warehouse.replace_resideo_oauth_token(
            account=RESIDEO_ACCOUNT,
            token_json=json.dumps(token, sort_keys=True, separators=(",", ":")),
            updated_at=datetime.now(tz=UTC),
        )
    finally:
        warehouse.close()
    print("Installed the Resideo credential.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
