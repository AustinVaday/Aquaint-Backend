class _Storage:
    def __init__(self):
        self.sources = []
        self.size = 0
    
    def __shrink(self):
        self.sources = filter(lambda x: len(x) > 0, self.sources)
     
    def load(self, source):
        self.sources.append(source)
        self.size += len(source)
    
    def __len__(self):
        return len(self.sources)
    
    def __getitem__(self, index):
        col = self.sources[index]
        return col[len(col)-1]
    
    def pop(self, index):
        if size > 0:
            ret = self.sources[index].pop()
            self.size -= 1
            self.__shrink()
            return ret
        else:
            return None


class Sorter:
    def __init__(self, key):
        self.key = key
        self.storage = _Storage()
    
    def load(self, source):
        if len(source) > 0:
            self.storage.load(source)
    
    def size(self):
        return self.storage.size
    
    def pop(self):
        thresh = 0
        index = None
        for i in range(0, len(self.storage)):
            if self.key(self.storage[i]) > thresh:
                index = i
                thresh = self.key(self.storage[i])
            
        return self.storage.pop(index)
