import luigi
import os

def getFileLength(inputFileName):
    file_length = 0
    with open(inputFileName) as infile:
        for i in infile:
            file_length += 1
    return file_length

class readFile(luigi.Task):
    inputFile = luigi.Parameter()
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(self.inputFile)

class Looper(luigi.Task):
    inputFile = luigi.Parameter()
    start = luigi.Parameter()
    end = luigi.Parameter()
    def requires(self):
        return readFile(inputFile=self.inputFile)
    def output(self):
        outfileName = 'looper_out/Looper_' + self.start + '_' + self.end + '.task'
        return luigi.LocalTarget(outfileName)
    def run(self):
        output = []
        iterator = 0
        with open(self.inputFile, 'r') as infile, self.output().open('w') as outfile:
            iterator = 0
            for i,line in enumerate(infile):
                # if i >= (int(self.start) - 1):
                if i >= (int(self.start)-1) and i <= (int(self.end)-1):
                    # print("i -> " + str(i) + ", line -> " + line)
                    output.append(str(line))
                iterator = iterator + 1
            outfile.write("".join(output))

class Process(luigi.Task):
    inputFile = luigi.Parameter()
    start = luigi.Parameter(default="1")
    end = luigi.Parameter(default="1000")
    def requires(self):
        taskArray = []
        i=1
        file_length = getFileLength(self.inputFile)
        while(i<file_length):
            taskArray.append(Looper(start=str(i), end=str((i+(int(self.end)-int(self.start)))), inputFile=self.inputFile))
            i=i+int(self.end)
        return taskArray

class ProcessMultiple(luigi.Task):
    inputFile = luigi.Parameter()
    def requires(self):
        return [
        Looper(start="1", end="10", inputFile=self.inputFile),
        Looper(start="11", end="20", inputFile=self.inputFile),
        Looper(start="21", end="30", inputFile=self.inputFile),
        ]

class ProcessOne(luigi.Task):
    inputFile = luigi.Parameter()
    def requires(self):
        Looper(start="1", end="100", inputFile=self.inputFile)

if __name__ == '__main__':
    luigi.run()
