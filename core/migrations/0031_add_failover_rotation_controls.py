from django.db import migrations


PROXY_SETTINGS_KEY = "proxy_settings"
DEFAULTS = {
    "stream_connection_attempts": 3,
    "min_failover_rotation_interval": 10,
}


def update_settings(apps, schema_editor, remove=False):
    CoreSettings = apps.get_model("core", "CoreSettings")
    alias = schema_editor.connection.alias
    try:
        obj = CoreSettings.objects.using(alias).get(key=PROXY_SETTINGS_KEY)
    except CoreSettings.DoesNotExist:
        return

    if not isinstance(obj.value, dict):
        return

    value = dict(obj.value)
    for key, default in DEFAULTS.items():
        if remove:
            value.pop(key, None)
        else:
            value.setdefault(key, default)
    obj.value = value
    obj.save(using=alias, update_fields=["value"])


def forwards(apps, schema_editor):
    update_settings(apps, schema_editor)


def backwards(apps, schema_editor):
    update_settings(apps, schema_editor, remove=True)


class Migration(migrations.Migration):
    dependencies = [("core", "0030_add_upstream_read_timeout")]
    operations = [migrations.RunPython(forwards, backwards)]

