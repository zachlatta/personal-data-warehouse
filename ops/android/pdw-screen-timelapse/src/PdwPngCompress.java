package com.zachlatta.pdw.screentimelapse;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import java.io.File;
import java.io.FileOutputStream;

/**
 * Command-line helper invoked by {@code pdw ingest screen-timelapse --compress-png}
 * on Android via {@code /system/bin/app_process64}.
 *
 * <p>Usage: {@code PdwPngCompress <quality> <maxEdge> <in.png> <out.webp> [<in2.png> <out2.webp> ...]}
 */
public final class PdwPngCompress {
    public static void main(String[] args) throws Exception {
        if (args.length < 4 || (args.length - 2) % 2 != 0) {
            System.err.println(
                "usage: PdwPngCompress <quality> <maxEdge> <in.png> <out.webp> [...]"
            );
            System.exit(2);
        }
        int quality = Integer.parseInt(args[0]);
        int maxEdge = Integer.parseInt(args[1]);
        if (quality < 1 || quality > 100) {
            System.err.println("quality must be 1..100");
            System.exit(2);
        }
        for (int i = 2; i < args.length; i += 2) {
            String inPath = args[i];
            String outPath = args[i + 1];
            Bitmap bmp = BitmapFactory.decodeFile(inPath);
            if (bmp == null) {
                System.err.println("could not decode PNG: " + inPath);
                System.exit(1);
            }
            try {
                int w = bmp.getWidth();
                int h = bmp.getHeight();
                int maxDim = Math.max(w, h);
                if (maxEdge > 0 && maxDim > maxEdge) {
                    int nw = Math.max(1, (w * maxEdge) / maxDim);
                    int nh = Math.max(1, (h * maxEdge) / maxDim);
                    Bitmap scaled = Bitmap.createScaledBitmap(bmp, nw, nh, true);
                    if (scaled != bmp) {
                        bmp.recycle();
                        bmp = scaled;
                    }
                }
                File outFile = new File(outPath);
                try (FileOutputStream fos = new FileOutputStream(outFile)) {
                    if (!bmp.compress(Bitmap.CompressFormat.WEBP_LOSSY, quality, fos)) {
                        System.err.println("Bitmap.compress(WEBP_LOSSY) returned false for " + inPath);
                        System.exit(1);
                    }
                }
                if (outFile.length() == 0) {
                    System.err.println("encoded empty output for " + inPath);
                    System.exit(1);
                }
            } finally {
                bmp.recycle();
            }
        }
    }
}
