import plotly.graph_objs as go
from plotly.offline import init_notebook_mode, plot, iplot


def in_ipython_notebook():
    try:
        ipython_class = get_ipython().__class__.__name__
        if 'ZMQInteractiveShell' in ipython_class:
            return True
        else:
            return False
    except NameError:
        return False


class PlotlyFigure:
    """Represents a Plotly figure and provides helper methods for plotting.

    Attributes:
        __notebook_mode: Indicates if the plotly notebook mode has been initialized.

    """

    __notebook_mode = False

    def __init__(self, plotly_traces=None, plotly_layout=None):
        if plotly_traces:
            self.data = plotly_traces
        else:
            self.data = []
        self.layout = plotly_layout

        self.title = ""
        self.title_x = ""
        self.title_y = ""

    @property
    def figure(self):
        if self.layout is None:
            self.layout = go.Layout(
                title=self.title,
                xaxis=dict(
                    rangeslider=dict(
                        visible=False
                    ),
                    title=self.title_x
                ),
                yaxis=dict(
                    title=self.title_y
                )
            )
        return go.Figure(data=self.data, layout=self.layout)

    def set_title(self, title, title_x="", title_y=""):
        self.title = title
        self.title_x = title_x
        self.title_y = title_y
        return self

    def to_html(self):
        return plot(self.figure, auto_open=False, output_type='div')

    def to_file(self):
        raise NotImplementedError

    def plot(self):
        if not self.__notebook_mode:
            init_notebook_mode(connected=True)
            self.__notebook_mode = True
        iplot(self.figure)
        return self

    def candle_stick(self, df, name=None):
        if not self.title_x:
            self.title_x = "Time"
        if not self.title_y:
            self.title_y = "Value"

        trace = go.Candlestick(
            x=df.timestamp,
            open=df.open,
            high=df.high,
            low=df.low,
            close=df.close,
            name=name
        )
        self.data.append(trace)

        return self

    def line(self, x, y, name=None):
        trace = go.Scatter(
            x=x,
            y=y,
            name=name
        )
        self.data.append(trace)
        return self

    def lines(self, x, *args):
        """

        Args:
            x:
            *args:

        Returns:

        """
        y_list = []
        name_list = []
        for arg in args:
            if isinstance(arg, str):
                name_list.append(arg)
            else:
                y_list.append(arg)

        for i in range(len(y_list)):
            trace = go.Scatter(
                x=x,
                y=y_list[i],
                name=name_list[i] if i < len(name_list) else None
            )
            self.data.append(trace)
        return self

