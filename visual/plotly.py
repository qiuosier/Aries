import plotly.graph_objs as go
from plotly.offline import init_notebook_mode, plot, iplot


def in_ipython_notebook():
    """Checks if the program is running in a ipython/jupyter notebook.
    """
    try:
        ipython_class = get_ipython().__class__.__name__
        if 'ZMQInteractiveShell' in ipython_class:
            return True
        else:
            return False
    except NameError:
        return False


def init_notebook():
    """Initialize plotly in a jupyter notebook for each cell.
    In Jupyter notebook, plotly can be initialized for the entire notebook using
    init_notebook_mode(connected=True)
    However, in Google Colab, the initialization needs to be done in each cell.
    IPython allows user to register event callbacks for each cell.
    This cell register the initialization as the "pre_execute" event callback.
    See Also: https://ipython.readthedocs.io/en/stable/config/callbacks.html
    """
    # Import IPython here so that it is not required.
    import IPython
    def init_plotly():
        display(IPython.core.display.HTML(
            '''
            <script src="/static/components/requirejs/require.js"></script>
            <script>
            requirejs.config({
                paths: {
                base: '/static/base',
                plotly: 'https://cdn.plot.ly/plotly-latest.min.js?noext',
                },
            });
            </script>
            '''
        ))
    IPython.get_ipython().events.register('pre_execute', init_plotly)


class PlotlyFigure:
    """Represents a Plotly figure and provides helper methods for plotting.

    Many methods in this class support "Method Chaining", i.e. they return the PlotlyFigure instance itself.

    Attributes:
        __notebook_mode: Indicates if the plotly notebook mode has been initialized.

    Examples:
        PlotlyFigure().candle_stick(ts.df, "Daily Data").lines(tech_df).plot()

    """

    __notebook_mode = False

    def __init__(self, plotly_traces=None, plotly_layout=None, **kwargs):
        if plotly_traces:
            self.data = plotly_traces
        else:
            self.data = []
        self.layout = plotly_layout
        self.layout_args = kwargs

        self.title = ""
        self.title_x = ""
        self.title_y = ""

    @staticmethod
    def __is_pandas_data_frame(x):
        """Checks if x and args are pandas data frames using strings to avoid importing pandas.
        So that pandas is not required in order to use this function.

        Args:
            x: A variable.

        Returns: True if "pandas" and "DataFrame" are both in the type/class name of x

        """
        from pandas import DataFrame
        return issubclass(x.__class__, DataFrame)

    @staticmethod
    def __is_pandas_data_series(x):
        from pandas import Series
        return issubclass(x.__class__, Series)

    @property
    def figure(self):
        """Prepares the plotly figure.
        """
        if self.layout is None:
            self.layout = go.Layout(
                title=self.title,
                xaxis=dict(
                    rangeslider=dict(
                        visible=False
                    ),
                    title=self.title_x,
                    automargin=True
                ),
                yaxis=dict(
                    title=self.title_y,
                    automargin=True
                ),
                **self.layout_args
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
        """Plots the figure in a jupyter notebook
        """
        if not self.__notebook_mode:
            init_notebook_mode(connected=True)
            self.__notebook_mode = True
        iplot(self.figure)
        return self

    # The following methods support "Method Chaining"

    def candle_stick(self, df, name=None, **kwargs):
        if not self.title_x:
            self.title_x = "Time"
        if not self.title_y:
            self.title_y = "Value"

        trace = go.Candlestick(
            x=df.index,
            open=df.open,
            high=df.high,
            low=df.low,
            close=df.close,
            name=name,
            **kwargs
        )
        self.data.append(trace)

        return self

    def line(self, x, y, name=None, **kwargs):
        trace = go.Scatter(
            x=x,
            y=y,
            name=name,
            **kwargs
        )
        self.data.append(trace)
        return self

    def __parse_args(self, names, array, *args):
        for arg in args:
            if isinstance(arg, str):
                names.append(arg)
            else:
                array.append(arg)
        return names, array

    def lines(self, x, *args, **kwargs):
        """

        Args:
            x:
            *args:

        Returns:

        """
        y_list = []
        y_name = []

        if self.__is_pandas_data_frame(x):
            df = x
            x = df.index
            for c in df.columns.values:
                y_list.append(df[c])
                y_name.append(c)
        elif self.__is_pandas_data_series(x):
            series = x
            x = series.index
            y_list.append(series)
            y_name.append(series.name)
        else:
            y_name, y_list = self.__parse_args(y_name, y_list, *args)
        for i in range(len(y_list)):
            trace = go.Scatter(
                x=x,
                y=y_list[i],
                name=y_name[i] if i < len(y_name) else None,
                **kwargs
            )
            self.data.append(trace)
        return self

    def bar(self, x, y, name=None, **kwargs):
        trace = go.Bar(x=x, y=y, name=name, **kwargs)
        self.data.append(trace)
        return self

    def add_trace(self, trace_type, **kwargs):
        trace = getattr(go, trace_type)(**kwargs)
        self.data.append(trace)
        return self
