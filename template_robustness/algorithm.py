"""The template robustness algorithm."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Self
import itertools
import networkx as nx
from tqdm import tqdm


class IsolationLevel(Enum):
    """Enum representing the different isolation levels."""
    READ_COMMITTED = 1
    SNAPSHOT_ISOLATION = 2
    SERIALIZABLE = 3


class Conn(Enum):
    """Three possible connectedness options: connected to o1, connected to p1, or not connected."""
    O = 1
    P = 2
    N = 3


class InOut(Enum):
    """Two possible labelling options for nodes in the graph: in or out."""
    IN = 1
    OUT = 2


@dataclass(frozen=True, eq=True)
class Operation:
    """Dataclass representing an operation."""
    variable: str
    relation: str
    readset: frozenset[str] = frozenset()
    writeset: frozenset[str] = frozenset()

    def is_rw_conflicting(self, other: Self) -> bool:
        """True iff this operation is rw-conflicting with the other operation."""
        return self.relation == other.relation and \
            len(self.readset.intersection(other.writeset)) > 0

    def is_wr_conflicting(self, other: Self) -> bool:
        """True iff this operation is wr-conflicting with the other operation."""
        return self.relation == other.relation and \
            len(self.writeset.intersection(other.readset)) > 0

    def is_ww_conflicting(self, other: Self) -> bool:
        """True iff this operation is ww-conflicting with the other operation."""
        return self.relation == other.relation and \
            len(self.writeset.intersection(other.writeset)) > 0

    def is_conflicting(self, other: Self) -> bool:
        """True iff this operation is conflicting with the other operation."""
        return self.is_rw_conflicting(other) or \
            self.is_wr_conflicting(other) or \
            self.is_ww_conflicting(other)


@dataclass(frozen=True, eq=True)
class Template:
    """Dataclass representing a template."""
    name: str
    operations: list[Operation] = field(compare=False)


@dataclass
class TemplateSet:
    """Dataclass representing a set of templates."""
    templates: set[Template]


@dataclass
class Allocation:
    """Dataclass representing an allocation over a set of templates."""
    templateset: TemplateSet
    mapping: dict[Template, IsolationLevel]

    def __repr__(self):
        result = "Allocation:\n"
        for t, il in self.mapping.items():
            result += f"\t{t.name}: {il}\n"
        return result


@dataclass(frozen=True, eq=True)
class GraphNode:
    """Dataclass representing a node in the pt-conflict-graph."""
    template: Template
    operation: Operation
    conn: Conn
    k: InOut


def is_node_valid(node: GraphNode, o1: Operation, p1: Operation, t1: Template) -> bool:
    """Check whether a node is valid in the pt-conflict-graph."""
    if node.conn == Conn.N:
        return True

    # Conditions (1) and (2)
    for op1 in t1.operations:
        if (op1.variable == o1.variable and node.conn == Conn.O) or \
                (op1.variable == p1.variable and node.conn == Conn.P):
            for op in node.template.operations:
                if op.variable == node.operation.variable:
                    if op1.is_conflicting(op):
                        return False
    return True


def is_edge_valid(node1: GraphNode, node2: GraphNode, h: int) -> bool:
    """Check whether an edge is valid in the pt-conflict-graph."""
    # Conflict between two templates
    if node1.k == InOut.OUT and node2.k == InOut.IN:
        # Condition (3)
        if node1.conn == node2.conn and \
                node1.operation.is_conflicting(node2.operation):
            return True

    # pass info within template
    elif node1.k == InOut.IN and node2.k == InOut.OUT and node1.template == node2.template:
        # Condition (4)
        if node1.operation.variable != node2.operation.variable and \
            (node1.conn, node2.conn) in [(Conn.O, Conn.P), (Conn.O, Conn.N),
                                         (Conn.N, Conn.N), (Conn.N, Conn.P)]:
            return True

        # Condition (5)
        if node1.operation.variable == node2.operation.variable and \
                node1.conn == node2.conn:
            return True

        # Condition (6)
        if node1.operation.variable == node2.operation.variable and \
                node1.conn == Conn.O and node2.conn == Conn.P and \
                h == 1:
            return True

    return False


def pt_conflict_graph(o1: Operation, p1: Operation, t1: Template,
                      h: int, template_set: TemplateSet) -> nx.Graph:
    """Construct the pt-conflict-graph(o1, p1, t1, h, template_set)."""
    graph = nx.Graph()

    # Create nodes
    for t in template_set.templates:
        for o in t.operations:
            for c in Conn:
                for k in InOut:
                    node = GraphNode(t, o, c, k)
                    if is_node_valid(node, o1, p1, t1):
                        graph.add_node(node)

    # Add edges
    for node1, node2 in itertools.product(graph.nodes, repeat=2):
        if is_edge_valid(node1, node2, h):
            graph.add_edge(node1, node2)

    return graph


def reachable(t2: Template, o2: Operation, p2: Operation, co2: Conn,
              tn: Template, on: Operation, pn: Operation, cpn: Conn,
              h: int, rtc: nx.Graph) -> bool:
    """Verify whether we can close the sequence loop from t2 to tn."""
    # Case 1: n = 2 => t2 = tn
    if t2 == tn and o2 == on and p2 == pn:
        # o2 = on is connected with p1 and pn = p2 is connected with o1
        if co2 == Conn.P and cpn == Conn.O:
            return True
        # Note: if variables of o2 and p2 are the same, the algorithm will propagate connectedness.
        # i.e., o2 is connected to o1 and pn is connected to p1.
        # This is fine, if we assume o1 and p1 to be connected (i.e., h = 1)
        if h == 1 and co2 == Conn.O and cpn == Conn.P:
            return True

    # Case 2: n = 3 => direct conflict between t2 and t3 = tn
    if o2.is_conflicting(pn):
        # Connectedness correctly propagated
        if co2 == cpn:
            return True
        # Special case: assumption that o1 and p1 are connected (i.e., h = 1)
        if h == 1 and co2 == Conn.O and cpn == Conn.P:
            return True

    # Case 3: n > 3 => Use transitive closure
    # iterate over edges in rtc, and check compatibility with operations o2 and pn.
    for nstart, nstop in rtc.edges:
        if nstart.k == InOut.IN and nstop.k == InOut.OUT and \
                nstart.operation.is_conflicting(o2) and nstop.operation.is_conflicting(pn) and \
                nstart.conn == co2 and nstop.conn == cpn:
            return True

    return False


def get_connectedness(target_op: Operation,
                      o: Operation, co: Conn,
                      p: Operation, cp: Conn,
                      h: int) -> set[Conn]:
    """
    Returns the set of connectedness options for the target operation in the provided template.
    The incoming and outgoing operations o and p are provided, together with their connectedness.
    If h=1 (i.e., o and p are connected), the connectedness of the target operation is
    automatically extended to both Conn.O and Conn.P if at least one of both is included.
    """
    result: set[Conn] = set()
    if target_op.variable == o.variable:
        result.add(co)
    if target_op.variable == p.variable:
        result.add(cp)
    if len(result) == 0:
        result = {Conn.N, }

    if h == 1:
        if Conn.O in result:
            result.add(Conn.P)
        if Conn.P in result:
            result.add(Conn.O)

    # Some sanity checks
    assert (h == 1 and result in [{Conn.N, }, {Conn.O, Conn.P}]) or \
        (h == 2 and result in [{Conn.N, }, {Conn.O, }, {Conn.P, }])

    return result


def is_valid_cycle(t1: Template, o1: Operation, p1: Operation,
                   t2: Template, o2: Operation, p2: Operation, co2: Conn,
                   tn: Template, on: Operation, pn: Operation, cpn: Conn,
                   h: int, alloc: Allocation) -> bool:
    """
    Check whether the cycle is valid.
    Note that this only requires info about templates t1, t2, and tn
    """
    # Condition (2) and (3): no ww conflict between prefix of t1 and either t2 or tn
    # Furthermore, if t1 is under SI or SSI, no ww conflict between
    # postfix of t1 and either t2 or tn
    for op1 in t1.operations:
        op1_conns = get_connectedness(op1, o1, Conn.O, p1, Conn.P, h)
        # No need to check ww-conflicts if op1 is not connected to o1 and p1
        if Conn.N not in op1_conns:
            for op2 in t2.operations:
                if op1.is_ww_conflicting(op2):
                    op2_conns = get_connectedness(op2, o2, co2, p2, Conn.O, h)
                    if len(op1_conns.intersection(op2_conns)) > 0:
                        # Connected!
                        return False
            for opn in tn.operations:
                if op1.is_ww_conflicting(opn):
                    opn_conns = get_connectedness(opn, on, Conn.P, pn, cpn, h)
                    if len(op1_conns.intersection(opn_conns)) > 0:
                        # Connected!
                        return False
        if op1 == o1 and alloc.mapping[t1] == IsolationLevel.READ_COMMITTED:
            # Only operations before or equal to o1 in t1 are considered
            break

    # Condition (4)
    if not o1.is_rw_conflicting(p2):
        return False

    # Condition (5)
    if not on.is_rw_conflicting(p1):
        if alloc.mapping[t1] != IsolationLevel.READ_COMMITTED:
            return False
        for op in t1.operations:
            # p1 occurs before o1 in t1
            if op == p1:
                return False
            if op == o1:
                break

    # Condition (6)
    if alloc.mapping[t1] == IsolationLevel.SERIALIZABLE and \
            alloc.mapping[t2] == IsolationLevel.SERIALIZABLE and \
            alloc.mapping[tn] == IsolationLevel.SERIALIZABLE:
        return False

    # Condition (7)
    if alloc.mapping[t1] == IsolationLevel.SERIALIZABLE and \
            alloc.mapping[t2] == IsolationLevel.SERIALIZABLE:
        for op1 in t1.operations:
            op1_conns = get_connectedness(op1, o1, Conn.O, p1, Conn.P, h)
            for op2 in t2.operations:
                if op1.is_wr_conflicting(op2):
                    op2_conns = get_connectedness(op2, o2, co2, p2, Conn.O, h)
                    if len(op1_conns.intersection(op2_conns)) > 0:
                        # Connected!
                        return False

    # Condition (8)
    if alloc.mapping[t1] == IsolationLevel.SERIALIZABLE and \
            alloc.mapping[tn] == IsolationLevel.SERIALIZABLE:
        for op1 in t1.operations:
            op1_conns = get_connectedness(op1, o1, Conn.O, p1, Conn.P, h)
            for opn in tn.operations:
                if op1.is_rw_conflicting(opn):
                    opn_conns = get_connectedness(opn, on, Conn.P, pn, cpn, h)
                    if len(op1_conns.intersection(opn_conns)) > 0:
                        # Connected!
                        return False

    return True


def is_robust(template_set: TemplateSet, alloc: Allocation) -> tuple[bool,dict]:
    """Returns True if the set of templates is robust under the given allocation"""
    template_ops = {(t, o, p) for t in template_set.templates for o, p in itertools.product(t.operations, repeat=2)}
    for t1, o1, p1 in tqdm(template_ops):
    #for t1 in template_set.templates:
    #    for o1, p1 in tqdm(itertools.product(t1.operations, repeat=2)):
        h_options = {1, } if o1.variable == p1.variable else {1, 2}
        for h in h_options:
            pt_graph = pt_conflict_graph(o1, p1, t1, h, template_set)
            rtc = nx.transitive_closure(pt_graph, reflexive=True)
            for t2, p2 in {(t, p) for t in template_set.templates for p in t.operations}:
                if not o1.is_rw_conflicting(p2):
                    continue
                for o2 in t2.operations:
                    for tn, on in {(t, p) for t in template_set.templates for p in t.operations}:
                        if not on.is_conflicting(p1):
                            continue
                        for pn in tn.operations:
                            co2_options = {Conn.O, } if o2.variable == p2.variable else {
                                Conn.N, Conn.P}
                            cpn_options = {Conn.P, } if on.variable == pn.variable else {
                                Conn.N, Conn.O}
                            for co2, cpn in itertools.product(co2_options, cpn_options):
                                if is_valid_cycle(t1, o1, p1, t2, o2, p2, co2,
                                                        tn, on, pn, cpn, h, alloc) and \
                                                            reachable(t2, o2, p2, co2, tn, on, pn, cpn, h, rtc):
                                    return (False, {
                                        "t1": t1,
                                        "o1": o1,
                                        "p1": p1,
                                        "h": h,
                                        "t2": t2,
                                        "o2": o2,
                                        "p2": p2,
                                        "co2": co2,
                                        "tn": tn,
                                        "on": on,
                                        "pn": pn,
                                        "cpn": cpn
                                    })
    return (True, {})


def optimal_alloc(template_set) -> Allocation:
    """Returns the optimal robust allocation for the provided set of templates"""
    alloc = Allocation(template_set,
                       {t: IsolationLevel.SERIALIZABLE for t in template_set.templates})

    for t in template_set.templates:
        print(f"Processing template {t.name}")
        # Try SI
        alloc.mapping[t] = IsolationLevel.SNAPSHOT_ISOLATION
        if not is_robust(template_set, alloc)[0]:
            # not robust -> revert
            alloc.mapping[t] = IsolationLevel.SERIALIZABLE
        else:
            # robust -> Try RC
            alloc.mapping[t] = IsolationLevel.READ_COMMITTED
            if not is_robust(template_set, alloc)[0]:
                # Not robust -> revert
                alloc.mapping[t] = IsolationLevel.SNAPSHOT_ISOLATION
    return alloc
