import luigi

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def arrayOfTasks(inputFile):

class readFile(luigi.Task):
    inputFile = luigi.Parameter()
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(self.inputFile)

class subLooper(luigi.Task):
    inputFile = luigi.Parameter()
    start = luigi.Parameter()
    end = luigi.Parameter()
    def requires(self):
        return readFile(inputFile=self.inputFile)
    def output(self):
        return luigi.LocalTarget('subTask'+'_'+start+'_'+end+'.task')
    def run(self):
        with self.input() as inFile, self.output() as outFile:
            for i,line in enumerate(inFile):
                if i >= (self.start-1):
                    outFile.write(line)
                elif i <= (self.end-1):
                    outFile.write(line)

class looper(luigi.Task):
    inputFile = luigi.Parameter
    def requires(self):
        arrayOfTasks = []
        with (inputFile, 'r') as infile:
            length = file_len(infile)
            # print("Debug Line -> Length: " + length)
            decLength = length
            while i < length:
                start = i + 1
                end = i + 100
                # print("Debug Line -> i: " + i + " start: " + start + " end: " + end)
                arrayOfTasks.append(subLooper(inputFile=self.inputFile, start=start, end=end))
                i = i + 100
                decLength = decLength + 100
                # print("Debug Line -> i: " + i + " decLength: " + decLength)
                if(decLength < 100):
                    start = i + 1
                    end = length
                    print("Debug Line -> i: " + i + " start: " + start + " end: " + end)
                    arrayOfTasks.append(subLooper(inputFile=self.inputFile, start=start, end=end)
            # print("Debug Line -> arrayOfTasks: " + len(arrayOfTasks))
        return arrayOfTasks

if __name__ == '__main__'
    print "running"
    luigi.run()
