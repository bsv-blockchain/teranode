# Install DB

## Sqlite
POC db engine of choice is Sqlite. This is a lightweight and simple engine that perfectly serves the needs of CON component.

### How to install Sqlite
```bash
brew install sqlite3 sqlite-utils
```

### Obejct Relational Mapping
To speed up the development of the store logic we choose a proven Go ORM framework - GORM. We need to install two packages:
1. gorm
2. sqlite driver

```bash
go get gorm.io/driver/sqlite
go get gorm.io/gorm
```

We have created placeholders for two additional db engines: _postgres_ and _mongodb_. At this point we are not planning on adding support for these engines, but the requirements may change in the future.
