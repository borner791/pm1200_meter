import threading
import queue
import time

class batcher(threading.Thread):
    def __init__(self, batchSize):
        self.dataIn = queue.Queue(20)
        self.runThread = True
        self.npoints = 0
        self.BATCH_SIZE = batchSize
        self.batch = []

        threading.Thread.__init__(self)
        self.start()
    
    def fresh_data(self, data):
        self.dataIn.put_nowait(data)

    def quit(self):
        self.runThread = False
        self.npoints = 0
        self.dataIn.put_nowait(1) #make sure we're not waiting on an empty queue


    def run(self):
        while self.runThread:
            self.batch.append(self.dataIn.get()) #Blocks until there is new data
            self.npoints += 1
            if self.npoints >= self.BATCH_SIZE:
                #send it
                
                print("send it")
                for h in self.batch:
                    print(h)
                self.batch = []
                self.npoints = 0
        
if __name__ == "__main__":
    batch = batcher(5)
    batch.fresh_data("h")
    batch.fresh_data("e")
    batch.fresh_data("l")
    batch.fresh_data("l")
    batch.fresh_data("o")
    time.sleep(5)
    batch.quit()
    batch.join()