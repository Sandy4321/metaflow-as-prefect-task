from metaflow import FlowSpec, step, Parameter
from datetime import datetime
from random import random


class SummationFlow(FlowSpec):
    """
    SummationFlow is a minimal DAG showcasing reading data from a previous Prefect task,
    do some calculations with a numerical library and terminate the flow successfully.
    """

    SUM = Parameter(
        name='sum',
        help='Result of Prefect summation task',
        default=1
    )

    @step
    def start(self):
        """
        Read params in.
        """
        print("SUM is {}".format(self.SUM))
        self.series = [random() for _ in range(self.SUM)]
        self.next(self.multiply)


    @step
    def multiply(self):
        """
        As a sample ML-task, we import a numerical library and do some stuff.
        """
        import numpy as np
        self.avg = np.average(self.series)
        print("Average is: {}".format(self.avg))
        self.next(self.end)


    @step
    def end(self):
        """
        DAG is done: say goodbye!
        """
        print('Dag ended at {}. See you, space cowboy!'.format(datetime.utcnow()))


if __name__ == '__main__':
    SummationFlow()
