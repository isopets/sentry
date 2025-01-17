from sentry.grouping.component import GroupingComponent
from sentry.grouping.strategies.base import produces_variants, strategy
from sentry.interfaces.template import Template


@strategy(ids=["template:v1"], interface=Template, score=1100)
@produces_variants(["default"])
def template_v1(template, event, context, **meta):
    filename_component = GroupingComponent(id="filename")
    if template.filename is not None:
        filename_component.update(values=[template.filename])

    context_line_component = GroupingComponent(id="context-line")
    if template.context_line is not None:
        context_line_component.update(values=[template.context_line])

    return {
        context["variant"]: GroupingComponent(
            id="template", values=[filename_component, context_line_component]
        )
    }
