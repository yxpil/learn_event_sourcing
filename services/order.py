"""Order aggregate: state is built by replaying events; commands append new events."""
from framework.eventstore import EventStore


class Order:
    def __init__(self, store: EventStore, order_id: str):
        self.store = store
        self.order_id = order_id
        self.state = {"order_id": order_id, "status": "NEW",
                       "user_id": None, "amount": 0, "balance_deducted": 0, "stock_deducted": 0}
        self._replay()

    def _replay(self):
        for evt in self.store.load(self.order_id):
            self._apply(evt["type"], evt["payload"])

    def _apply(self, etype, p):
        s = self.state
        if etype == "OrderCreated":
            s["user_id"] = p["user_id"]; s["amount"] = p["amount"]; s["status"] = "CREATED"
        elif etype == "BalanceDeducted":
            s["balance_deducted"] = p["amount"]; s["status"] = "PAID"
        elif etype == "StockDeducted":
            s["stock_deducted"] = p["qty"]
        elif etype == "OrderCancelled":
            s["status"] = "CANCELLED"

    # commands: produce events, never mutate state directly
    def create(self, user_id, amount):
        self.store.append(self.order_id, "OrderCreated", {"user_id": user_id, "amount": amount})
        self._apply("OrderCreated", {"user_id": user_id, "amount": amount})

    def deduct_balance(self, amount):
        self.store.append(self.order_id, "BalanceDeducted", {"amount": amount})
        self._apply("BalanceDeducted", {"amount": amount})

    def deduct_stock(self, qty):
        self.store.append(self.order_id, "StockDeducted", {"qty": qty})
        self._apply("StockDeducted", {"qty": qty})

    def cancel(self):
        self.store.append(self.order_id, "OrderCancelled", {})
        self._apply("OrderCancelled", {})


def project_user_orders(store: EventStore):
    """Read model: replay all streams to build a per-user order summary."""
    summary = {}
    for oid, events in store.all_streams().items():
        user_id = None; total = 0; status = None
        for e in events:
            if e["type"] == "OrderCreated":
                user_id = e["payload"]["user_id"]; total = e["payload"]["amount"]; status = "CREATED"
            elif e["type"] == "BalanceDeducted":
                status = "PAID"
            elif e["type"] == "OrderCancelled":
                status = "CANCELLED"
        if user_id:
            summary.setdefault(user_id, []).append({"order_id": oid, "total": total, "status": status})
    return summary
