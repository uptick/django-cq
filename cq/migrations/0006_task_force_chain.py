# -*- coding: utf-8 -*-
# Generated by Django 1.9.4 on 2016-10-10 00:03
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cq', '0005_task_last_retry'),
    ]

    operations = [
        migrations.AddField(
            model_name='task',
            name='force_chain',
            field=models.BooleanField(default=False),
        ),
    ]
