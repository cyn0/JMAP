ID_LENGTH = 8

class JMapperUtil:
    def __init__(self, jmapper):
        self.Jmapper = jmapper
        self.prefix_int_map = {}

    def build_memory_lookup(self):
        lookup_rows = self.Jmapper.read_lookup(None)
        if lookup_rows is None:
            return

        for row in lookup_rows:
            id = str(row[0])
            field = str(row[1])
            level = row[2]
            if len(id) == 7:
                id = "0" +id
            level_field_val = id[:(level+1)*2]

            prefix = level_field_val[:-2]
            cur_val = level_field_val[-2:]

            if prefix not in self.prefix_int_map or (prefix in self.prefix_int_map and self.prefix_int_map[prefix] < cur_val):
                self.prefix_int_map[prefix] = int(cur_val)

        print "#"*100
        for item in self.prefix_int_map:
            print item + "-->" + str(self.prefix_int_map[item])
        print "#" * 100


    def get_prefix_current_int(self, prefix):
        if prefix in self.prefix_int_map:
            return self.prefix_int_map[prefix]
        else:
            return 0

    def append_field_path(self, prefix, curr):
        if prefix is None:
            return str(curr)
        else:
            return str(prefix + "." + curr)

    def incr_prefix_current_int(self, prefix):
        cur_val = 0
        if prefix in self.prefix_int_map:
            cur_val = self.prefix_int_map[prefix]
        self.prefix_int_map[prefix] = cur_val + 1
