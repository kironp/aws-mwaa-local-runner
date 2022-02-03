class Row:

    def __init__(self, headers, values):
        self.row_data = {}
        for elem in zip(headers, values):
            k, v = elem
            self.row_data.update({k: v})

    def __getitem__(self, item):
        return self.row_data[item]

    def __getattr__(self, item):
        return self.row_data[item]

    def __repr__(self):
        return str(self.row_data)
    
    def get_dict(self):
        return self.row_data