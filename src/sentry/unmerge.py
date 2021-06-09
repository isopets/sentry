import abc
import dataclasses
from dataclasses import dataclass
from typing import Any, Collection, Mapping, Optional, Sequence, Tuple, Union

from sentry import eventstream
from sentry.eventstore.models import Event
from sentry.models.grouphash import GroupHash
from sentry.models.project import Project

_DEFAULT_UNMERGE_KEY = "default"


class UnmergeReplacement(abc.ABC):
    """
    A type defining how and by which criteria a subset of events can be
    moved out of a group into a new, different group.

    Right now only one concrete implementation exists, the "classical" unmerge.
    In the future there will be an additional concrete type for splitting up
    groups based on hierarchical_hashes column.
    """

    @staticmethod
    def parse_arguments(fingerprints: Any = None, replacement: Any = None) -> "UnmergeReplacement":
        if replacement is not None:
            assert isinstance(replacement, UnmergeReplacement)
            return replacement
        elif fingerprints is not None:
            return PrimaryHashUnmergeReplacement(fingerprints=fingerprints)
        else:
            raise TypeError("Either fingerprints or replacement argument is required.")

    @abc.abstractmethod
    def get_unmerge_key(
        self, event: Event, locked_primary_hashes: Collection[str]
    ) -> Optional[str]:
        """
        The unmerge task iterates through all events of a group. This function
        should return which of them should land in the new group.

        If the issue should be moved, a string should be returned. Events with
        the same string are moved into the same issue.
        """

        raise NotImplementedError()

    @abc.abstractproperty
    def primary_hashes_to_lock(self) -> Collection[str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def start_snuba_replacement(
        self, project: Project, source_id: int, unmerge_key: str, destination_id: int
    ) -> Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def stop_snuba_replacement(self, eventstream_state: Any) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def run_postgres_replacement(
        self,
        project: Project,
        unmerge_key: str,
        destination_id: int,
        locked_primary_hashes: Collection[str],
    ) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_activity_args(self, unmerge_key: str) -> Mapping[str, Any]:
        raise NotImplementedError()

    def on_finish(self, project: Project, source_id: int):
        pass


@dataclass(frozen=True)
class PrimaryHashUnmergeReplacement(UnmergeReplacement):
    """
    The "classical unmerge": Moving events out of the group based on primary_hash.
    """

    fingerprints: Collection[str]

    def get_unmerge_key(
        self, event: Event, locked_primary_hashes: Collection[str]
    ) -> Optional[str]:
        primary_hash = event.get_primary_hash()
        if primary_hash in self.fingerprints and primary_hash in locked_primary_hashes:
            return _DEFAULT_UNMERGE_KEY

        return None

    @property
    def primary_hashes_to_lock(self) -> Collection[str]:
        return self.fingerprints

    def start_snuba_replacement(
        self, project: Project, source_id: int, unmerge_key: str, destination_id: int
    ) -> Any:
        return eventstream.start_unmerge(project.id, self.fingerprints, source_id, destination_id)

    def stop_snuba_replacement(self, eventstream_state: Any) -> None:
        if eventstream_state:
            eventstream.end_unmerge(eventstream_state)

    def run_postgres_replacement(
        self,
        project: Project,
        unmerge_key: str,
        destination_id: int,
        locked_primary_hashes: Collection[str],
    ) -> None:
        # Move the group hashes to the destination.
        GroupHash.objects.filter(project_id=project.id, hash__in=locked_primary_hashes).update(
            group=destination_id
        )

    def get_activity_args(self, unmerge_key: str) -> Mapping[str, Any]:
        assert unmerge_key == _DEFAULT_UNMERGE_KEY
        return {"fingerprints": self.fingerprints}


@dataclass(frozen=True)
class HierarchicalUnmergeReplacement(UnmergeReplacement):
    """
    Split up one issue by a particular entry in `hierarchical_hashes` into many
    issues.

    In contrast to `PrimaryHashUnmergeReplacement`, this produces multiple new
    groups instead of just one.
    """

    primary_hash: str
    filter_hierarchical_hash: str
    filter_level: int
    new_level: int
    assume_source_emptied: bool
    reset_hashes: Sequence[str]

    def get_unmerge_key(
        self, event: Event, locked_primary_hashes: Collection[str]
    ) -> Optional[str]:
        if event.get_primary_hash() != self.primary_hash:
            return None

        hierarchical_hashes = event.data.get("hierarchical_hashes")

        if not hierarchical_hashes:
            return None

        try:
            if hierarchical_hashes[self.filter_level] != self.filter_hierarchical_hash:
                return None
        except IndexError:
            return None

        try:
            return hierarchical_hashes[self.new_level]
        except IndexError:
            return hierarchical_hashes[-1]

    @property
    def primary_hashes_to_lock(self) -> Collection[str]:
        return set()

    def start_snuba_replacement(
        self, project: Project, source_id: int, unmerge_key: str, destination_id: int
    ) -> Any:
        return eventstream.start_unmerge_hierarchical(
            project_id=project.id,
            primary_hash=self.primary_hash,
            hierarchical_hash=unmerge_key,
            previous_group_id=source_id,
            new_group_id=destination_id,
            skip_needs_final=self.assume_source_emptied,
        )

    def stop_snuba_replacement(self, eventstream_state: Any) -> None:
        if eventstream_state:
            eventstream.end_unmerge_hierarchical(eventstream_state)

    def run_postgres_replacement(
        self,
        project: Project,
        unmerge_key: str,
        destination_id: int,
        locked_primary_hashes: Collection[str],
    ) -> None:
        if self.reset_hashes:
            GroupHash.objects.filter(
                project_id=project.id, hash__in=self.reset_hashes, state=GroupHash.State.SPLIT
            ).update(state=GroupHash.State.UNLOCKED)

        GroupHash.objects.update_or_create(
            project=project, hash=unmerge_key, defaults={"group_id": destination_id}
        )

    def get_activity_args(self, unmerge_key: str) -> Mapping[str, Any]:
        return {
            "new_hierarchical_hash": unmerge_key,
        }

    def on_finish(self, project: Project, source_id: int):
        if self.assume_source_emptied:
            eventstream.exclude_groups(project.id, [source_id])


@dataclass(frozen=True)
class UnmergeArgsBase:
    """
    Parsed arguments of the Sentry unmerge task. Since events of the source
    issue are processed in batches, one can think of each batch as belonging to
    a state in a statemachine.

    That statemachine has only two states: Processing the first page
    (`InitialUnmergeArgs`), processing second, third, ... page
    (`SuccessiveUnmergeArgs`). On the first page postgres hashes are migrated,
    activity models are created, eventstream and pagination state is
    initialized, and so the successive tasks need to carry significantly more
    state with them.
    """

    project_id: int
    source_id: int
    replacement: UnmergeReplacement
    actor_id: int
    batch_size: int

    @staticmethod
    def parse_arguments(
        project_id: int,
        source_id: int,
        destination_id: Optional[int],
        fingerprints: Sequence[str],
        actor_id: Optional[int],
        last_event: Optional[str] = None,
        batch_size: int = 500,
        source_fields_reset: bool = False,
        eventstream_state: Any = None,
        replacement: Optional[UnmergeReplacement] = None,
        locked_primary_hashes: Optional[Collection[str]] = None,
        destinations: Optional[Mapping[str, int]] = None,
    ) -> "UnmergeArgs":
        if destinations is None:
            if destination_id is not None:
                destinations = {_DEFAULT_UNMERGE_KEY: (destination_id, eventstream_state)}
            else:
                destinations = {}

        if last_event is None:
            assert eventstream_state is None
            assert not source_fields_reset

            return InitialUnmergeArgs(
                project_id=project_id,
                source_id=source_id,
                replacement=UnmergeReplacement.parse_arguments(fingerprints, replacement),
                actor_id=actor_id,
                batch_size=batch_size,
                destinations=destinations,
            )
        else:
            assert locked_primary_hashes is not None or fingerprints is not None
            return SuccessiveUnmergeArgs(
                project_id=project_id,
                source_id=source_id,
                replacement=UnmergeReplacement.parse_arguments(fingerprints, replacement),
                actor_id=actor_id,
                batch_size=batch_size,
                last_event=last_event,
                destinations=destinations,
                locked_primary_hashes=locked_primary_hashes or fingerprints or [],
                source_fields_reset=source_fields_reset,
            )

    def dump_arguments(self) -> Mapping[str, Any]:
        rv = dataclasses.asdict(self)
        rv["fingerprints"] = None
        rv["destination_id"] = None
        rv["replacement"] = self.replacement
        return rv


@dataclass(frozen=True)
class InitialUnmergeArgs(UnmergeArgsBase):
    # In tests the destination task is passed in explicitly from the outside,
    # so we support unmerging into an existing destination group. In production
    # this does not happen.
    destinations: Mapping[str, Tuple[int, Optional[Mapping[str, Any]]]]


@dataclass(frozen=True)
class SuccessiveUnmergeArgs(UnmergeArgsBase):
    last_event: Optional[Any]
    locked_primary_hashes: Collection[str]

    # unmerge may only start mutating data on a successive page, once it
    # actually has found an event that needs to be migrated.
    # (unmerge_key) -> (group_id, eventstream_state)
    destinations: Mapping[str, Tuple[int, Optional[Mapping[str, Any]]]]

    # likewise unmerge may only find "source" events (events that should not be
    # migrated) on the second page, only then (and only once) it can reset
    # group attributes such as last_seen.
    source_fields_reset: bool


UnmergeArgs = Union[InitialUnmergeArgs, SuccessiveUnmergeArgs]