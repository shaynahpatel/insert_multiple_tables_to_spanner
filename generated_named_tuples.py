from apache_beam import coders
from typing import NamedTuple, List, Optional
import datetime
class SpannerRow_Order(NamedTuple):
	orderId : int
	order_status : Optional[str] = None
class SpannerRow_Product(NamedTuple):
	productId : int
	product_name : Optional[str] = None
class SpannerRow_Supplier(NamedTuple):
	supplierId : int
	supplier_name : Optional[str] = None
