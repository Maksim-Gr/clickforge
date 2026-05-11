Output docs/explain.gif
Set FontSize 14
Set Width 900
Set Height 500
Set Theme "Dracula"

# Requires a live ClickHouse instance; swap host/database as needed
Type `schemamaker explain "SELECT * FROM events WHERE user_id = 123" --host localhost`
Enter
Sleep 2s
