from collections import Iterable
from sentry.mediators import Mediator, Param
from sentry.models import Rule


class Creator(Mediator):
    name = Param((str,))
    environment = Param(int, required=False)
    team = Param("sentry.models.Team", required=False)
    user = Param("sentry.models.User", required=False)
    project = Param("sentry.models.Project")
    action_match = Param((str,))
    filter_match = Param((str,), required=False)
    actions = Param(Iterable)
    conditions = Param(Iterable)
    frequency = Param(int)
    request = Param("rest_framework.request.Request", required=False)

    def call(self):
        self.rule = self._create_rule()
        return self.rule

    def _create_rule(self):
        kwargs = self._get_kwargs()
        rule = Rule.objects.create(**kwargs)
        return rule

    def _get_kwargs(self):
        data = {
            "filter_match": self.filter_match,
            "action_match": self.action_match,
            "actions": self.actions,
            "conditions": self.conditions,
            "frequency": self.frequency,
        }
        _kwargs = {
            "label": self.name,
            "team": self.team,
            "user": self.user,
            "environment_id": self.environment or None,
            "project": self.project,
            "data": data,
        }
        return _kwargs
