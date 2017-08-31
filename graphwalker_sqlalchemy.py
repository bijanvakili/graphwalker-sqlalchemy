import hashlib
import inspect

from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.ext.declarative import clsregistry


# maps Sequelize relations to multiplicities
_RELATION_TYPE_MAP = {
    'MANYTOMANY': '*..*',
    'MANYTOONE': '*..1',
    'ONETOMANY': '1..*',
    'ONETOONE': '1..1',
    'inheritance': '1..1'
}


def _make_hash_id(v):
    return hashlib.sha1(v.encode('utf-8')).hexdigest()


def get_vertex_key(cls_orm_model):
    return '{}.{}'.format(cls_orm_model.__module__, get_class_name(cls_orm_model))


def get_class_name(cls_orm_model):
    if getattr(cls_orm_model, 'class_', None):
        return cls_orm_model.class_.__name__
    elif getattr(cls_orm_model, '__name__', None):
        return cls_orm_model.__name__
    else:
        return str(cls_orm_model)


def _r_extract_vertices(cls_orm_model, vertex_map: dict, fq_vertex_labels=False):
    qualified_model_name = get_vertex_key(cls_orm_model)
    vertex_id = _make_hash_id(qualified_model_name)

    # avoid cycles
    if vertex_id in vertex_map:
        return

    if fq_vertex_labels:
        vertex_name = qualified_model_name
    else:
        vertex_name = get_class_name(cls_orm_model)

    # extract the current vertex
    base_class_names = [get_class_name(b) for b in inspect.getmro(cls_orm_model)]
    vertex_map[vertex_id] = {
        'id': vertex_id,
        'label': vertex_name,
        'searchableComponents': [
            get_class_name(cls_orm_model)
        ],
        'properties': {
            'model_name': get_class_name(cls_orm_model),
            'module_name': cls_orm_model.__module__,
            'base_classes': base_class_names
        }
    }

    # recurse
    for child_class in cls_orm_model.__subclasses__():
        _r_extract_vertices(child_class, vertex_map)


def _r_extract_edges(cls_orm_model, visited: set, edge_map: dict):
    # add edges for relationships
    source_qualified_model_name = get_vertex_key(cls_orm_model)
    if source_qualified_model_name in visited:
        return
    else:
        visited.add(source_qualified_model_name)

    source_vertex_id = _make_hash_id(source_qualified_model_name)

    try:
        mapper = sa_inspect(cls_orm_model)
    except NoInspectionAvailable:
        mapper = None
    if mapper:
        for relation_name in mapper.relationships.keys():
            relation = mapper.relationships[relation_name]

            target = relation.argument
            if isinstance(target, clsregistry._class_resolver):
                target = target()

            dest_qualified_model_name = get_vertex_key(target)
            dest_vertex_id = _make_hash_id(dest_qualified_model_name)

            back_reference = relation.backref
            if type(back_reference) == tuple:
                back_reference = back_reference[0]

            relation_type = relation.direction.name

            edge_qname = '{}({},{})'.format(
                relation_type,
                source_qualified_model_name,
                dest_qualified_model_name
            )
            edge_id = _make_hash_id(edge_qname)

            field_properties = {
                'name': relation_name,
                'back_reference': back_reference,
                'source_columns': [c.name for c in relation.local_columns],
                'dest_columns': [c.name for c in relation.remote_side]
            }

            existing_edge = edge_map.get(edge_id)
            if not existing_edge:
                edge = {
                    'id': edge_id,
                    'label': None,
                    'source': source_vertex_id,
                    'dest': dest_vertex_id,
                    'properties': {
                        'type': relation_type,
                        'is_self_referential': relation._is_self_referential,
                        'fields': {
                            relation_name: field_properties
                        },
                        'multiplicity': _RELATION_TYPE_MAP[relation_type]
                    }
                }
                edge_map[edge_id] = edge
            else:
                existing_edge['properties']['fields'][relation_name] = field_properties

    # add edges for inheritance
    relation_type = 'inheritance'
    for child_class in cls_orm_model.__subclasses__():
        dest_qualified_model_name = get_vertex_key(child_class)
        dest_vertex_id = _make_hash_id(dest_qualified_model_name)

        edge_qname = '{}({},{})'.format(
            relation_type,
            source_qualified_model_name,
            dest_qualified_model_name
        )
        edge_id = _make_hash_id(edge_qname)
        existing_edge = edge_map.get(edge_id)
        if not existing_edge:
            edge = {
                'id': edge_id,
                'label': None,
                'source': source_vertex_id,
                'dest': dest_vertex_id,
                'properties': {
                    'type': relation_type,
                    'is_self_referential': False
                }
            }
            edge_map[edge_id] = edge

    # recurse
    for child_class in cls_orm_model.__subclasses__():
        _r_extract_edges(child_class, visited, edge_map)


def extract(root_orm_class, fq_vertex_labels=False):
    vertex_map = {}
    _r_extract_vertices(root_orm_class, vertex_map, fq_vertex_labels=fq_vertex_labels)

    edge_map = {}
    visited = set()
    _r_extract_edges(root_orm_class, visited, edge_map)

    # compute new labels for edges
    for edge in edge_map.values():
        if edge['properties']['type'] == 'inheritance':
            label = 'inheritance'
        else:
            label = ', '.join(sorted(edge['properties']['fields'].keys()))
            multiplicity = edge['properties'].get('multiplicity')
            if multiplicity:
                label += ' ({})'.format(multiplicity)
        edge['label'] = label

    return {
        'vertices': list(vertex_map.values()),
        'edges': list(edge_map.values())
    }
