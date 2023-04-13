import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String}, out={"stocks": Out(dagster_type=List[Stock], description="List of Stock")})
def get_s3_data_op(context):
    """
    using s3_key context (currently a static csv file) 
    and the csv_helper function, this creates a list of Stock
    """

    return list(csv_helper(context.op_config['s3_key']))


@op(
    ins={"stock_list": In(dagster_type=List[Stock], description="List of Stock")},
    out={"aggregation": Out(dagster_type=Aggregation,
                            description="Aggregated value from data")}
)
def process_data_op(context, stock_list):
    """
    using context from previous op and the returned stock_list, 
    this op takes the stock list converting from Stock class 
    and turning into a dictionary to turn into a Pandas DataFrame. 
    Then it's just a matter of finding the max 
    and pulling out the required Aggregation class attributes 
    and readjusting their datatype
    """

    df = pd.DataFrame([dict(s) for s in stock_list])
    high_date, high_value = df.loc[df.high == max(
        df.high)]['date'], df.loc[df.high == max(df.high)]['high']

    return Aggregation(
        date=datetime(
            int(high_date.dt.year),
            int(high_date.dt.month),
            int(high_date.dt.day)
        ),
        high=high_value)


@op(
    ins={"Aggregation": In(dagster_type=Aggregation,
                            description="highest value of stock")},
    out={"nothing": Out(dagster_type=Nothing,
                            description="Nothing for now, thank you!")}
)
def put_redis_data_op(context, Aggregation):
    pass


@op(
    
    ins={"Aggregation": In(dagster_type=Aggregation,
                            description="highest value of stock")},
    out={"nothing": Out(dagster_type=Nothing,
                            description="Nothing for now, thank you!")}
)
def put_s3_data_op(context, Aggregation):
    pass


@job
def machine_learning_job():

    a = get_s3_data_op()

    b = process_data_op(a)

    redis_line = put_redis_data_op(b)

    s3_line = put_s3_data_op(b)

