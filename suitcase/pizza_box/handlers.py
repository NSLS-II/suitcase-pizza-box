import pandas as pd
import os

# These transformations are the transformations necessary 
# to convert the hex values that come from the ADC's into units of Volts.
fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc
enc2counts = lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1)


class PizzaBoxANHandler():
    
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list for specific file

        Parameters
        ----------
        resource_path: str
            tells the computer where to find the file
        chunk_size: int (optional)
            user specifices size of chunk for data, default is 1024
        '''
        self._name = resource_path
        self.chunks_of_data = []
        chunk = [data for data in pd.read_csv(resource_path, 
            chunksize=chunk_size, delimiter = " ", header = None) ]
        
        num_cols = len(chunk[0].columns)

        columns = ['time (s)', 'time (ns)', 'index']
        columns_leftover = num_cols - len(columns)
        columns = columns + [f'adc {i}' for i in range(columns_leftover)]
        
        for chunk in chunk:
            chunk.columns = columns
            
            for column in range(columns_leftover):
                chunk.iloc[:, column + 3] = chunk.iloc[:, column + 3].apply(adc2counts)
            
            chunk['timestamp'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            column_keys = ['timestamp'] + [f'adc {i}' for i in range(columns_leftover)]
            chunk = chunk[column_keys]
            self.chunks_of_data.append(chunk)    
        

    def __call__(self, chunk_num, column):
        '''
        Returns 
        -------
        result: dataframe object
            specified chunk number/index from list of all chunks created
        '''
        cols = {
                'timestamp': self.chunks_of_data[chunk_num]['timestamp'],
                'counts': self.chunks_of_data[chunk_num][f'adc {column}']
               }
        return pd.DataFrame(cols, columns = ['timestamp', 'counts'])
         
        

    def get_file_size(self, datum_kwarg_gen):
        filename = f'{self._name}'
        size = os.path.getsize(filename)
               
        return size


class PizzaBoxENHandler():
    
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list for specific file

        Parameters
        ----------
        resource_path: str
            tells the computer where to find the file
        chunk_size: int (optional)
            user specifices size of chunk for data, default is 1024
        '''
        self._name = resource_path
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, chunksize=chunk_size, 
                names = ['time (s)', 'time (ns)', 'encoder', 'index', 'di'], 
                delimiter = " ", header=None):
            chunk['timestamp'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            chunk['encoder'] = chunk['encoder'].apply(enc2counts)
            chunk = chunk.drop(columns = ['time (s)', 'time (ns)', 'index', 'di'])
            chunk = chunk[['timestamp', 'encoder']]
            self.chunks_of_data.append(chunk)


    def __call__(self, chunk_num, column):
        '''
        Returns 
        -------
        result: dataframe object
            specified chunk number/index from list of all chunks created
        '''
        result = self.chunks_of_data[chunk_num]
        return result


    def get_file_size(self, datum_kwarg_gen):
        filename = f'{self._name}'
        size = os.path.getsize(filename)
               
        return size