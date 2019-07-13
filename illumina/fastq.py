import os
import re
from ..visual.plotly import PlotlyFigure
from ..utils import sort_lists


class FASTQFile:
    def __init__(self, file_path):
        self.file_path = file_path
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found at %s." % file_path)
        
    def group_by_barcode(self):
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
        return barcode_dict

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
