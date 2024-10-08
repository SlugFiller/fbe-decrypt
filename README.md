# Utility to decrypt drive images encrypted with Android's File-Based Encryption (FBE)

This utility is specifically geared for decrypting Android emulator images. It is intended for Android version 11+ (API version 30+). For Android versions 9.0 or earlier, which use Full-Disk Encryption (FDE) you will need a [different tool](https://faui1-gitlab.cs.fau.de/gaston.pugliese/avdecrypt/-/tree/master).

This script also doubles as the most complete documentation of FBE, as the official documentation only paints broad strokes, and the source code is spread across several different projects.

## Usage

First, you will need to [install NodeJS](https://nodejs.org/en/download/package-manager/all). Then, open a command line terminal, and navigate to the location of the emulator image:
```
cd ~/.android/avd/MyDevice.avd
```

Finally, run the script:
```
node /path/to/fbe-encrypt.mjs
```

The script will create a file called `userdata-decrypted.img` in the current directory. This is an Ext4 partition image file. It can be mounted as a drive, or opened with an application that can read Ext4 images, e.g. 7-Zip.

## Assumptions

The script does not take any parameters, and makes several assumptions regarding the image. These assumption hold for a default Android image, but may vary based on the installed ROM. These assumptions include:
 - The drive is encrypted with FBE, and not FDE (Enforced since Android 10)
 - The drive is encrypted with metadata encryption (Enforced since Android 11)
 - The device is not locked with a PIN or any other authentication method
 - The drive is encrypted using AES-256-XTS. ROMs for low-powered devices may use Adiantum instead
 - The drive uses the Ext4 file system. Some ROMs may use the F2FS file system
 - The synthetic password uses version 3 encryption and key derivation (In use since Android 10)
 - The files are encrypted using policy version 2 of "Linux native file encryption", also known as "fscrypt" (Is the default for Android 11+)
 - The synthetic password's device-bound key is stored in `persistent.sqlite` and not in `1000_USRSKEY_synthetic_password_*` (Applies as of Android 12). Alternately, there is no synthetic password (Applies to older Android versions when not locked with a PIN)
 - Device keys are encrypted using the software driver, and not using special hardware like chips supporting the Weaver API. This will necessarily be true for an emulator image, but might not apply to an actual device image

Comments within the source code show where these assumptions are made, and give hints on how the code may be changed to support other configurations.

## Important

Before decrypting an image, make sure you shut down the emulator _using the emulated power button_. The emulator emulates a full system including a disk write cache. If the device is not shut down in a way the emulated system recognizes, some blocks may be left unwritten to disk. This could result in a failure to decrypt, or in a corrupt decrypted image.
