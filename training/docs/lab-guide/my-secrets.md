# 🔑 My secrets

**My secrets** is your folder in the lab's key vault (OpenBao). Keep API keys, passwords and other
values there that must not be written into a notebook or a DAG. Code asks for a secret by name when it
needs it, so the value never sits in your files and is never printed. Only you can read your folder.
Your Jupyter server gets a vault login that reaches it and nothing else, and a new one at every start.

## Add a secret
In the **Add a secret** card:

1. Choose **Single value** or **Several fields**.
2. Type a **NAME**: letters, digits and `_`, not starting with a digit, up to 64 (e.g. `WEATHER_API_KEY`).
3. Type the **value**. For **Several fields**, give each field a name and a value (it starts with
   `username` and `password`; **+ Add field** adds one, **✕** removes one).
4. Leave **secret** ticked for keys and passwords: the value is never shown again, only replaced.
   Untick it for a plain setting you want to see on the page (e.g. a region).
5. **Save**.

| Kind | Use it for | Read it with |
|---|---|---|
| **Single value** | one key or token | `key_vault("NAME")` |
| **Several fields** | a whole login or connection, e.g. `db` with `username` and `password` | `key_vault("db", "password")` or `key_vault_dict("db")` |

You can have up to 50 secrets. A value holds up to 4,096 characters, and a secret up to 20 fields.

## Change or delete one
**Your secrets** lists each secret: **Name**, **Value** (hidden for a secret, with its field names for
one with several fields), **Updated (UTC)**.

- **Edit** fills the form (**Change <name>**). The name and kind stay. Leave a secret value empty to
  keep it, or type a new one. **Save**, or **Cancel**.
- **Delete**, then **Sure?** within 3 seconds, deletes it with all its versions.

## What the lab puts there itself
- **`kafka`**: your Kafka login (`username`, `password`, `bootstrap`), made with your first
  [📨 Pub/Sub stream](pubsub.md). `lab_kafka()` reads it. If you delete it, the lab puts it back within
  about 5 minutes. When you have no streams left, it goes.

## Read secrets in a notebook
The value is fetched when you ask for it: change it on the page, and the next call returns the new one,
without a restart.

```python
token = key_vault("WEATHER_API_KEY")     # a single value
pw    = key_vault("db", "password")      # one field of a secret with several
db    = key_vault_dict("db")             # every field, as a dict: {"username": ..., "password": ...}
key_vault_list()                         # the names of your secrets, never the values
key_vault_set("WEATHER_API_KEY", "abc")  # save one from code (secret=False: the page shows it)
```

A name you don't have raises `KeyError` (*no secret 'NAME' — add it on 🔑 My secrets*), and so does a
field the secret doesn't have. The older names `lab_secret`, `lab_secrets` and `lab_secret_set` still work.

In a **DAG** task, import it first; it reads the secrets of the member whose DAG it is:

```python
from lab_spark import key_vault
key = key_vault("WEATHER_API_KEY")
```

If the vault refuses (*the key vault refused your token*), your server's login has expired (it lasts a
week): **File → Hub Control Panel → Stop My Server → Start**.

## Team secrets
Once a manager has put you in a team, **👥 Team: <team>** at the top shows the team's folder,
`secret/<team>_team/`. Everyone in the team reads and changes these secrets, and no one else. It works
like your own: add, edit, delete. In code, add `team="<team>"`:

```python
token = key_vault("API_TOKEN", team="sales")
key_vault_list(team="sales")
key_vault_set("API_TOKEN", "abc123", team="sales")
```

In a team's own DAG, `key_vault("NAME")` reads the team's secrets without `team=`. After a move to
another team, restart your server to reach the new team's folder.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md#my-secrets-keys-and-passwords-in-a-key-vault)
- [Working in a team](../setup/teams.md#the-teams-secrets)
- [Consuming APIs with `requests`](../unit3/apis.md)
- [Airflow basics](../unit5/basics.md)
