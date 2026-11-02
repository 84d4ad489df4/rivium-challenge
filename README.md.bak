# rivium-challenge
rivium + apalis pocket backend challenge

Run a small background job queue **inside an Android app**, and trigger it with one cool Android feature.

**Time:** 1 week | **Level:** new to Rust is fine

## The idea

- **[Rivium](https://github.com/rivium-rs/rivium)** starts and stops your Rust code cleanly, on a laptop or inside an Android app.
- **[Apalis](https://github.com/apalis-dev/apalis)** runs background jobs (think "do this task later, retry if it fails").
- **An Android feature** is how jobs get started or how results show up on the phone.

Your goal: connect all three so that something useful happens on a real phone or emulator.

## What to build

A tiny Android app where:

1. Something on the phone creates a job (a button, a tile, a shared link, a photo, anything).
2. A Rust worker, running under Rivium, does the job.
3. The result shows up on the phone (notification, widget, screen, etc.).

## Must haves

- [ ] Rust code uses both Rivium and Apalis
- [ ] **One** job type is enough
- [ ] Runs on your laptop first with `cargo run` (no phone needed)
- [ ] Runs inside the Android app
- [ ] Jobs are saved, so they are still there after you close and reopen the app
- [ ] **One** Android feature from the list below
- [ ] A short README: what it does, how to run it, which versions you used

## Some Android feature examples

| Feature | Example |
|---|---|
| Quick Settings tile | Tap the tile to start a job |
| Notification | Show job progress or results |
| Home screen widget | Show how many jobs are waiting |
| Share Sheet | Share a link to your app and it gets processed |
| Camera / Photo Picker | Each photo becomes a job (resize, hash, etc.) |
| Your own idea | Tell us in an issue first |

## Suggested steps

1. Run Rivium's basic example:
   ```bash
   cargo run -p rivium-examples --example basic-service -- --root target/basic-example
   ```
2. Get an Apalis job running in a plain Rust program (no Android yet).
3. Put that worker inside a Rivium service.
4. Follow Rivium's JNI example to load it in Android.
5. Add your Android feature last.

Rivium needs Rust 1.92 or newer. Both libraries are still changing, so **pin the versions** you use and follow their current docs and examples.

## Scoring (100 points)

| What | Points |
|---|---:|
| It works end to end | 40 |
| The Android feature is creative and actually used | 30 |
| Jobs survive closing the app | 15 |
| README is clear | 15 |

**Bonus (+10):** test on a real phone and write down what happened. Rivium's own Android checks only confirm it compiles, so this is useful to the maintainers.

## Submit

A PR with your code, the README, and a short video or GIF of it working.

## Tips

- Stuck on Rust errors? Make it work on your laptop first, then move to Android.
- Keep it small. A simple idea that works beats a big one that doesn't.
- Questions about Rivium? Open an issue with the version, platform and a small example.
