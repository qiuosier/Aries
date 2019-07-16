import os
import re
from ..visual.plotly import PlotlyFigure
from ..utils import sort_lists


class FASTQFile:
    def __init__(self, file_path):
        self.file_path = file_path
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found at %s." % file_path)
        
    def group_by_barcode(self, threshold=0):
        barcode_pattern = r"[ACGTN]{8}\+[ACGTN]{8}"
        barcode_dict = dict()
        with open(self.file_path, 'r') as f:
            for i, line in enumerate(f, start=1):
                if not line.startswith("@"):
                    continue
                barcodes = line.strip().split(":")[-1]
                if re.match(barcode_pattern, barcodes):
                    line_list = barcode_dict.get(barcodes, [])
                    line_list.append(i)
                    barcode_dict[barcodes] = line_list
                else:
                    print("Line %d does not match barcode pattern" % i)
        if threshold > 0:
            barcode_dict = {k: v for k, v in barcode_dict.items() if len(v) > threshold}
        return barcode_dict

    def count_by_barcode(self, threshold=0):
        barcode_dict = self.group_by_barcode(threshold)
        return {k: len(v) for k, v in barcode_dict.items()}

    def barcode_histogram(self, max_bins=20):
        labels = []
        counts = []
        for k, v in self.group_by_barcode().items():
            labels.append(k)
            counts.append(len(v))
        counts, labels = sort_lists(counts, labels, reverse=True)
        if len(counts) > max_bins:
            counts = counts[:max_bins]
            labels = labels[:max_bins]
        return PlotlyFigure().add_trace("Histogram", x=labels, y=counts, histfunc='sum')


class BarcodeStatistics:
    def __init__(self, barcode_dict):
        self.barcode_dict = barcode_dict

    def filter_by_reads(self, threshold=0):
        self.barcode_dict = {k: v for k, v in self.barcode_dict.items() if v > threshold}
        return self

    def sort_data(self, max_size=0):
        labels = []
        counts = []
        for k, v in self.barcode_dict.items():
            labels.append(k)
            counts.append(v)
        counts, labels = sort_lists(counts, labels, reverse=True)
        if max_size and len(counts) > max_size:
            counts = counts[:max_size]
            labels = labels[:max_size]
        return counts, labels

    def histogram(self, max_bins=20):
        counts, labels = self.sort_data(max_bins)
        return PlotlyFigure().add_trace("Histogram", x=labels, y=counts, histfunc='sum')

    def bar_chart(self, max_size=20):
        counts, labels = self.sort_data(max_size)
        counts.reverse()
        labels.reverse()
        return PlotlyFigure(height=1000).bar(x=counts, y=labels, orientation='h')
