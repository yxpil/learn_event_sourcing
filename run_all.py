"""Demo: event sourcing — append events, replay state, build read models."""
import os, shutil
from framework.eventstore import EventStore
from services.order import Order, project_user_orders

DATA = os.path.join(os.path.dirname(__file__), "data")


def main():
    if os.path.exists(DATA):
        shutil.rmtree(DATA)
    os.makedirs(DATA, exist_ok=True)
    store = EventStore(os.path.join(DATA, "events.json"))

    # Case 1: create order + deduct balance + deduct stock, all as appended events
    print("--- Case 1: lifecycle as appended events ---")
    o = Order(store, "o_001")
    o.create(user_id="u_alice", amount=199.5)
    o.deduct_balance(199.5)
    o.deduct_stock(qty=1)
    print("current state (replayed):", o.state)

    # raw event log
    print("\nraw event log for o_001:")
    for e in store.load("o_001"):
        print(" ", e)

    # Case 2: a cancelled order
    print("\n--- Case 2: cancelled order ---")
    o2 = Order(store, "o_002")
    o2.create(user_id="u_bob", amount=50.0)
    o2.cancel()
    print("o_002 state:", o2.state)

    # Case 3: rebuild order state from scratch by replaying (new Order instance)
    print("\n--- Case 3: rebuild state by replaying (fresh instance) ---")
    o_reloaded = Order(store, "o_001")
    print("o_001 reloaded state:", o_reloaded.state)

    # Case 4: read model — per-user order summary built by replaying ALL streams
    print("\n--- Case 4: read model (per-user order summary) ---")
    summary = project_user_orders(store)
    for uid, orders in summary.items():
        print(f"  {uid}: {orders}")

    print("\n== all cases done ==")


if __name__ == "__main__":
    main()
