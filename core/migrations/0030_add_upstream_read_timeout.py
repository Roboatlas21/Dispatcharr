from django.db import migrations


PROXY_SETTINGS_KEY = "proxy_settings"


def add_upstream_read_timeout(apps, schema_editor):
    CoreSettings = apps.get_model("core", "CoreSettings")

    try:
        obj = CoreSettings.objects.get(key=PROXY_SETTINGS_KEY)
    except CoreSettings.DoesNotExist:
        return

    value = obj.value if isinstance(obj.value, dict) else {}
    value.setdefault("upstream_read_timeout", 10)

    obj.value = value
    obj.save()


def remove_upstream_read_timeout(apps, schema_editor):
    CoreSettings = apps.get_model("core", "CoreSettings")

    try:
        obj = CoreSettings.objects.get(key=PROXY_SETTINGS_KEY)
    except CoreSettings.DoesNotExist:
        return

    value = obj.value if isinstance(obj.value, dict) else {}
    value.pop("upstream_read_timeout", None)

    obj.value = value
    obj.save()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0029_add_failover_init_grace_period"),
    ]

    operations = [
        migrations.RunPython(
            add_upstream_read_timeout,
            remove_upstream_read_timeout,
        ),
    ]
