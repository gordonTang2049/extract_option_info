import numpy as np

class Util:
    def __init__(
        self, 
        nth_batch : int,
        batch_start : int,
        batch_end : int,
        batch_step : int):

        self.nth_batch = nth_batch
        self.batch_start = batch_start
        self.batch_end = batch_end
        self.batch_step = batch_step
        

    def batching(self):
        
        batch_arr = np.arange(
            self.batch_start,
            self.batch_step+self.batch_end,
            self.batch_step)
        
        batch_arr = np.repeat(batch_arr,2)[1:-1]

        num_of_batch = int((self.batch_start - self.batch_end) / self.batch_step)

        return np.reshape(batch_arr, (num_of_batch,2))[self.nth_batch]