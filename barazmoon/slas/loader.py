import os
import csv

TESTS_PATH = os.path.dirname(__file__)

path = os.path.join(TESTS_PATH, "report_bus_0001.log")


def slas_workload_generator(image_size: int, sla: int):
    image_size = image_size * 1024

    bandwidth = []
    with open(path, "r") as file:
        for line in file:
            elements = line.split()
            fifth_number = float(elements[4])
            sixth_number = float(elements[5])
            result = int(fifth_number / sixth_number)
            bandwidth.append(result)

    communication_cost = [image_size / x for x in bandwidth]
    dynamic_sla = [int(sla - x) for x in communication_cost]
    return dynamic_sla
