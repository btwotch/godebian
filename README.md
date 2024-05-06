# Go-Debian

Some go code to manage your debian installation; so far only apt-file is implemented.

### Usage:
```bash
$ cd cmd/go-apt-files
$ go build
$ ./go-apt-files search debian stable /usr/bin/g++
g++ | package info: {Name:g++ Version:debian/stable Depends:[] Filename:pool/main/g/gcc-defaults/g++_12.2.0-3_amd64.deb} | popularity: 1626
pentium-builder | package info: {Name:pentium-builder Version:debian/stable Depends:[] Filename:pool/main/p/pentium-builder/pentium-builder_0.21+nmu2_all.deb} | popularity: 46905
```
