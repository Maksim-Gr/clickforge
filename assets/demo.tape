# clickforge demo — render with:  vhs assets/demo.tape
# Self-contained: reads the bundled video_events.json, no external services needed.
# Build first so the binary is ready:  cargo build --release

Output assets/demo.gif

Set Shell "bash"
Set FontSize 19
Set Width 1240
Set Height 800
Set Padding 28
Set Theme "Catppuccin Mocha"
Set TypingSpeed 55ms

# Put the release binary on PATH so the prompt reads cleanly.
Hide
Type 'export PS1="$ " && export PATH="$PWD/target/release:$PATH" && clear'
Enter
Show

Sleep 800ms
Type "clickforge scan video_events.json"
Sleep 600ms
Enter
Sleep 4s

# scan ends with an interactive prompt — pick the ReplacingMergeTree suggestion.
Type "2"
Sleep 800ms
Enter
Sleep 2s

Type "head -n 20 video_events_up.sql"
Sleep 600ms
Enter
Sleep 4s

Sleep 1s
