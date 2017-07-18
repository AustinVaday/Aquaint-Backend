# Lazy sorting data structure
class _Storage:
    # Initialize empty storage
    def __init__(self):
        self.sources = []
        self.size = 0
    
    # Remove empty source lists
    def __shrink(self):
        self.sources = filter(lambda x: len(x) > 0, self.sources)
    
    # Add list to sources
    def load(self, source):
        self.sources.append(source)
        self.size += len(source)
    
    # Return number of sources
    def __len__(self):
        return len(self.sources)
    
    # Return the top value from the selected source
    def __getitem__(self, index):
        col = self.sources[index]
        return col[len(col)-1]
    
    # If available, pop the top value from the selected source
    def pop(self, index):
        if self.size > 0:
            ret = self.sources[index].pop()
            self.size -= 1
            self.__shrink()
            return ret
        else:
            return None

# Lazy sorter
class Sorter:
    # Initialize new sorter with storage
    def __init__(self, key):
        self.key = key
        self.storage = _Storage()
    
    # Add items to sorter
    def load(self, source):
        if len(source) > 0:
            self.storage.load(source)
    
    # Return number of items available to sort
    def size(self):
        return self.storage.size
    
    # Return the largest item from the top of each source in storage
    def pop(self):
        thresh = 0
        index = None
        
        # Find the largest item
        for i in range(0, len(self.storage)):
            if self.key(self.storage[i]) > thresh:
                index = i
                thresh = self.key(self.storage[i])
        
        # Pop the largest item
        return self.storage.pop(index)
