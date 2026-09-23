from django.db import migrations


PROXY_SETTINGS_KEY = "proxy_settings"


def add_failover_init_grace_period(apps, schema_editor):
    CoreSettings = apps.get_model("core", "CoreSettings")

    try:
        obj = CoreSettings.objects.get(key=PROXY_SETTINGS_KEY)
    except CoreSettings.DoesNotExist:
        return

    value = obj.value if isinstance(obj.value, dict) else {}
    value.setdefault("failover_init_grace_period", 30)

    obj.value = value
    obj.save()


def remove_failover_init_grace_period(apps, schema_editor):
    CoreSettings = apps.get_model("core", "CoreSettings")

    try:
        obj = CoreSettings.objects.get(key=PROXY_SETTINGS_KEY)
    except CoreSettings.DoesNotExist:
        return

    value = obj.value if isinstance(obj.value, dict) else {}
    value.pop("failover_init_grace_period", None)

    obj.value = value
    obj.save()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0028_alter_systemevent_event_type"),
    ]

    operations = [
        migrations.RunPython(
            add_failover_init_grace_period,
            remove_failover_init_grace_period,
        ),
    ]
