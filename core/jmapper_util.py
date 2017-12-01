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

    def getUpdateFieldId(self, lookup_rows, keyPath, fieldId = None, level = 1):
        if lookup_rows is None:
            return

        for item in keyPath:
            if fieldId is None:
                row = next(row for row in lookup_rows if str(row[1]) == item)
                fieldId = row[0]
                level = row[2]

            else:
                row = next(row for row in lookup_rows if str(row[1]) == item and row[0] > fieldId and row[0] < (self.getNextId(fieldId, int(level))))
                fieldId = row[0]
                level = row[2]
        return fieldId

    def get_prefix_current_int(self, prefix):
        if prefix in self.prefix_int_map:
            return self.prefix_int_map[prefix]
        else:
            return 0

    def incr_prefix_current_int(self, prefix):
        cur_val = 0
        if prefix in self.prefix_int_map:
            cur_val = self.prefix_int_map[prefix]
        self.prefix_int_map[prefix] = cur_val + 1

    def getNextId(self, currId, level):
        if len(str(currId)) == 7:
            currId = "0" + str(currId)
        level_field_val = currId[:(level + 1) * 2]
        no_of_zeros = ID_LENGTH - len(level_field_val)
        nextId = int(level_field_val) + 1

        nextId = int(str(nextId) + "0" * no_of_zeros)
        return nextId