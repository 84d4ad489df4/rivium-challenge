#!/usr/bin/env bash
# Build the LinkSpool Android debug APK.
#
# The script is self-contained: it locates the Android SDK/NDK, installs the
# required Rust targets, cross-compiles the JNI library, and bootstraps a
# pinned Gradle distribution before assembling the APK.
set -euo pipefail

app_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
android_dir="$app_dir/android"

log() {
  printf '[linkspool] %s\n' "$*" >&2
}

die() {
  printf '[linkspool] error: %s\n' "$*" >&2
  exit 1
}

# AGP 8.9 supports JDK 17 and 21; prefer a known-good JDK over a bleeding-edge
# default when the caller has not selected one.
if [[ -z "${JAVA_HOME:-}" || ! -x "${JAVA_HOME}/bin/java" ]]; then
  for candidate in \
    /usr/lib/jvm/java-21-openjdk-amd64 \
    /usr/lib/jvm/java-17-openjdk-amd64; do
    if [[ -x "$candidate/bin/java" ]]; then
      export JAVA_HOME="$candidate"
      break
    fi
  done
fi

sdk="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
if [[ -z "$sdk" || ! -d "$sdk" ]]; then
  for candidate in \
    "$HOME/Android/Sdk" \
    /usr/local/lib/android/sdk \
    /opt/android-sdk; do
    if [[ -d "$candidate" ]]; then
      sdk="$candidate"
      break
    fi
  done
fi
[[ -n "$sdk" && -d "$sdk" ]] || die "Android SDK not found; set ANDROID_HOME"
export ANDROID_HOME="$sdk"
export ANDROID_SDK_ROOT="$sdk"

ndk="${ANDROID_NDK_HOME:-}"
if [[ -z "$ndk" || ! -d "$ndk" ]]; then
  if [[ -d "$sdk/ndk" ]]; then
    ndk="$(find "$sdk/ndk" -mindepth 1 -maxdepth 1 -type d | sort -V | tail -n 1)"
  fi
fi
[[ -n "$ndk" && -d "$ndk" ]] || die "Android NDK not found; set ANDROID_NDK_HOME"

case "$(uname -s)" in
  Linux) host_tag="linux-x86_64" ;;
  Darwin) host_tag="darwin-x86_64" ;;
  *) die "unsupported host platform: $(uname -s)" ;;
esac
toolchain="$ndk/toolchains/llvm/prebuilt/$host_tag/bin"
[[ -d "$toolchain" ]] || die "NDK toolchain not found at $toolchain"

api_level="${LINKSPOOL_ANDROID_API:-26}"
command -v rustup >/dev/null 2>&1 || die "rustup is required to install Android targets"

targets=("aarch64-linux-android" "x86_64-linux-android")
installed="$(cd "$app_dir" && rustup target list --installed)"
for target in "${targets[@]}"; do
  if ! grep -qx "$target" <<<"$installed"; then
    log "installing Rust target $target"
    (cd "$app_dir" && rustup target add "$target")
  fi
done

rm -rf "$android_dir/app/src/main/jniLibs"
mkdir -p "$android_dir/app/src/main/jniLibs"

build_target() {
  local target="$1"
  local abi="$2"
  local clang_prefix="$3"
  local clang="$toolchain/${clang_prefix}${api_level}-clang"
  local env_target="${target//-/_}"

  [[ -x "$clang" ]] || die "NDK clang not found at $clang"

  log "building $target with $clang"
  export "CARGO_TARGET_${env_target^^}_LINKER=$clang"
  export "CC_${env_target}=$clang"
  export "AR_${env_target}=$toolchain/llvm-ar"
  export "RANLIB_${env_target}=$toolchain/llvm-ranlib"

  (cd "$app_dir" && cargo build --release --target "$target" -p linkspool-jni)

  mkdir -p "$android_dir/app/src/main/jniLibs/$abi"
  cp "$app_dir/target/$target/release/liblinkspool_jni.so" \
    "$android_dir/app/src/main/jniLibs/$abi/"

  # Rust's Android std links the NDK C++ runtime dynamically.
  local cxx_runtime="$ndk/toolchains/llvm/prebuilt/$host_tag/sysroot/usr/lib/$target/libc++_shared.so"
  if [[ -f "$cxx_runtime" ]]; then
    cp "$cxx_runtime" "$android_dir/app/src/main/jniLibs/$abi/"
  fi
}

build_target aarch64-linux-android arm64-v8a aarch64-linux-android
build_target x86_64-linux-android x86_64 x86_64-linux-android

gradle_version="${GRADLE_VERSION:-8.13}"
gradle_cache="${GRADLE_CACHE_DIR:-$HOME/.cache/linkspool-gradle}"
gradle_home="$gradle_cache/gradle-$gradle_version"
if [[ ! -x "$gradle_home/bin/gradle" ]]; then
  log "downloading Gradle $gradle_version"
  mkdir -p "$gradle_cache"
  archive="$gradle_cache/gradle-$gradle_version-bin.zip"
  curl -fsSL "https://services.gradle.org/distributions/gradle-$gradle_version-bin.zip" \
    -o "$archive"
  unzip -q "$archive" -d "$gradle_cache"
  rm -f "$archive"
fi

printf 'sdk.dir=%s\n' "$sdk" >"$android_dir/local.properties"

log "assembling the debug APK"
"$gradle_home/bin/gradle" \
  -p "$android_dir" \
  :app:assembleDebug \
  --no-daemon \
  --stacktrace

apk="$android_dir/app/build/outputs/apk/debug/app-debug.apk"
[[ -f "$apk" ]] || die "Gradle did not produce $apk"
cp "$apk" "$app_dir/debug.apk"
log "wrote $app_dir/debug.apk"
