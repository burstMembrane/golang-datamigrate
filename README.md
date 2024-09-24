# godatamigrate


Pins data to a version of a [gomigrate](https://github.com/DavidHuie/gomigrate) database
Hard fork of `gomigrate`

## TODO:


[x] Scaffold package
[x] take cli args with cobra
[x] Figure out how to parse a database version from the gomigrate files
[x] Validate the current version is current e.g all migrations have been applied
[x] Establish whether we need a DSL like [pgloader](https://github.com/dimitri/pgloader)
[x] Validate csv fields against migration
[x] Track versioning with `datamigrations` table
[x] Implement down and up commands
[x] Progress bar for CSV loading
[ ] Refactor (in progress)
  [ ] Refactor down, up and create commands to separate funcs
  [ ] Pretty cli logging and progress output

### Feat: s3 download
[ ] Figure out s3 downloading with go
[ ] Get CSV from s3
[ ] Stream/hold in memory
[ ] Conditionally load the CSV file from local or s3 location (optional)

