import uuid

class SerializableMixin(object):

    @classmethod
    def from_dict(cls,d,as_dict = False):

        refs = {}

        def deserialize(attributes):
            if isinstance(attributes,dict):
                d = {}
                for key,value in attributes.items():
                    d[key] = deserialize(value)
                if 'node_type' in attributes and not as_dict:
                    d = cls(d)
                if '__ref__' in d:
                    refs[d['__ref__']] = d
                    del d['__ref__']
                return d
            elif isinstance(attributes,(list,tuple)):
                return [deserialize(value) for value in attributes]
            return attributes

        def replace_references(attributes):
            if isinstance(attributes,(dict,cls)):
                for key,value in attributes.items():
                    if (isinstance(value,dict) or isinstance(value,cls)) and '__refto__' in value:
                        if not value['__refto__'] in refs:
                            raise AttributeError("unknown reference: %s" % value['__refto__'])
                        attributes[key] = refs[value['__refto__']]
                    else:
                        replace_references(value)
            elif  isinstance(attributes,(list,tuple)):
                [replace_references(value) for value in attributes]
            return attributes

        result = replace_references(deserialize(d))
        if not isinstance(result,cls) and not as_dict:
            return cls(result)
        return result

    def as_dict(self,with_children = True,serializer = None,walk = None,
                key_filter = None,exclude = None,use_refs = True,max_depth = None):

        """
        Serializes the object to a (self-referential) dictionary.
        """

        refs = {}
        unused_refs = {}

        class MaxDepthExceeded(BaseException):
            pass

        def serialize(attributes,with_children = True,depth = 0):
            if max_depth is not None and depth > max_depth:
                raise MaxDepthExceeded("Maximum depth exceeded")
            if isinstance(attributes,SerializableMixin):
                if walk is not None:
                    walk(attributes)
                if with_children:
                    if serializer is not None:
                        v = serializer(attributes)
                        return v
                    return serialize(attributes.attributes,with_children = with_children,depth = depth)
                else:
                    raise AttributeError
            elif isinstance(attributes,(list,tuple)):
                serialized_elements = []
                for element in attributes:
                    try:
                        serialized_elements.append(serialize(element,with_children = with_children,depth = depth+1))
                    except (AttributeError,MaxDepthExceeded):
                        continue
                return serialized_elements
            elif isinstance(attributes,dict):
                if '__ref__' in attributes:
                    if attributes['__ref__'] in unused_refs:
                        del unused_refs[attributes['__ref__']]
                    return {'__refto__' : attributes['__ref__']}

                d = {}
                if use_refs:
                    attributes['__ref__'] = uuid.uuid4().hex
                    refs[attributes['__ref__']] = attributes
                    unused_refs[attributes['__ref__']] = d

                for key,value in attributes.items():
                    if exclude is not None and key in exclude:
                        continue
                    if key_filter is not None:
                        if not key_filter(key) and not key == '__ref__':
                            continue
                    try:
                        d[key] = serialize(value,with_children = with_children,depth = depth+1)
                    except (AttributeError,MaxDepthExceeded):
                        pass
                return d
            return attributes
        try:
            if walk is not None:
                walk(self)
            s = serialize(self.attributes,with_children = with_children)
            #We remove the __ref__ fields that we've set.
            for key,value in refs.items():
                del value['__ref__']
            #We remove __ref__ fields from unused references in the serialized dictionaries
            for key,value in unused_refs.items():
                if '__ref__' in value:
                    del value['__ref__']
            return s
        except RuntimeError as e:
            raise
            