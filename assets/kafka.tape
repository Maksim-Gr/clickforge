# clickforge kafka demo — render with:  vhs assets/kafka.tape
# Self-contained: reads the bundled video_events.json, no external services needed.
# Build first so the binary is ready:  cargo build --release

Output assets/kafka.gif

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
Type "clickforge kafka video_events.json"
Sleep 600ms
Enter
Sleep 3s

# Show the generated pipeline DDL: the streams (Kafka engine) and raw tables.
Type "clear"
Enter
Sleep 300ms
Type "head -n 24 video_events_up.sql"
Sleep 600ms
Enter
Sleep 4s

Sleep 1s
