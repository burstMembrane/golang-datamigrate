# godatamigrate


Pins data to a version of a [gomigrate](https://github.com/DavidHuie/gomigrate) database
Hard fork of `gomigrate`

## TODO:


[x] Scaffold package
[x] take cli args with cobra
[x] Figure out how to parse a database version from the gomigrate files
[ ] Validate the current version is current e.g all migrations have been applied
[ ] Figure out s3 downloading with go
[ ] Establish whether we need a DSL like [pgloader](https://github.com/dimitri/pgloader)
[ ] Get CSV from s3
[ ] Validate csv fields against migration
[ ] Track versioning with `datamigrations` table
[ ] Implement down and up commands
