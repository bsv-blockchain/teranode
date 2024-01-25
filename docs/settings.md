# ⚙️Settings

All services accept settings allowing local and remote servers to have their own specific configuration.

The settings are stored in 2 files:

* `settings.conf` - Global settings
* `settings_local.conf` - Local overridden settings

The configuration system allows for a layered approach to settings. At its core, it works with a base setting. However, to cater to individualized or context-specific requirements, you can have context-dependent overrides.

Here's how it prioritizes:

1. `SETTING_NAME.context_name`: A context-specific override (highest priority).
2. `SETTING_NAME.base`: A general override.
3. `SETTING_NAME`: The base setting (lowest priority).

### Example

Suppose we have a base setting named `DATABASE_URL` to define the database connection URL for our application.

The base setting might be:
```
DATABASE_URL = "database-url-default.com"
```

Bob, a developer, might have his own database for development purposes. So, for the context `dev.bob`, there might be an override:
```
DATABASE_URL.dev.bob = "database-url-bob.com"
```

There might also be a generic development database URL, defined as:
```
DATABASE_URL.dev = "database-url-dev.com"
```

When Bob runs the application in his development context (`dev.bob`):

- The system first checks for `DATABASE_URL.dev.bob`. If it exists, it's used.
- If not, it falls back to the general development URL `DATABASE_URL.dev`.
- If neither exists, it defaults to `DATABASE_URL`.

For Bob, the resolution would be:

1. **First Preference:** `DATABASE_URL.dev.bob` -> "database-url-bob.com"
2. **Fallback:** `DATABASE_URL.dev` -> "database-url-dev.com"
3. **Last Resort:** `DATABASE_URL` -> "database-url-default.com"

Thus, with this approach, you have the flexibility to have a default setting, an optional general override, and further context-specific overrides. It's a hierarchical system that allows fine-grained control over configurations based on context.

---

### Accessing the Setttings from Go

The `gocore.Config()` offers various methods to retrieve settings from the configuration. Here's a guide on how to use them:

#### Initialization

Before using the configuration methods, ensure you've initialized and retrieved the configuration instance:

```go
config := gocore.Config()
```

#### Get

Retrieve a string value for a given key. If the key is not found, it will return the provided default value (if any) or an empty string.

```go
value, found := config.Get("someKey", "defaultValue")
```

#### GetMulti

Retrieve a slice of strings for a given key. Values should be separated by the provided separator.

```go
values, found := config.GetMulti("someKey", ",", []string{"default1", "default2"})
```

#### GetInt

Retrieve an integer value for a given key.

```go
intValue, found := config.GetInt("someKey", 42)
```

#### GetBool

Retrieve a boolean value for a given key.

```go
boolValue := config.GetBool("someKey", true)
```

#### GetURL

Retrieve a URL value for a given key. This method will also decrypt any encrypted tokens in the URL.

```go
urlValue, err, found := config.GetURL("someKey")
if err != nil {
	panic(err)
}
if !found {
	panic("URL config not found")
}
```

#### GetAll

Retrieve all configuration key-value pairs as a map.

```go
allConfigs := config.GetAll()
```

---

**Notes**:

1. The `Get`, `GetInt`, `GetMulti`, and `GetBool` methods all support an optional default value which will be returned if the key is not found in the configuration.
2. The `GetURL` method provides decryption for encrypted tokens in the URL, identified by the prefix `*EHE*`. If the URL is missing or invalid, an error will be returned.
3. Each method caches its results, ensuring that subsequent calls are efficient.
4. The configuration supports a context system. If a context is set (e.g., "dev.vicente"), the system will attempt to retrieve the setting specific to that context before falling back to a more general context or the base setting.

Remember to always check the returned `found` boolean or handle potential errors (like in the `GetURL` method) to ensure the expected configuration value is indeed retrieved.
