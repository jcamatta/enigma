import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import watchdog.events as watch_events


def on_created(event: watch_events.FileCreatedEvent):
    print(f"The file {event.src_path} was created")

def on_deleted(event: watch_events.FileDeletedEvent):
    print(f"The file {event.src_path} was deleted")

def on_modified(event: watch_events.FileModifiedEvent):
    print(f"The file {event.src_path} was modified")

def on_moved(event: watch_events.FileMovedEvent):
    print(f"The file {event.src_path} was moved")


def main():
    
    event_handler = PatternMatchingEventHandler(
    patterns=["*"],
    ignore_patterns=None,
    ignore_directories=True,
    case_sensitive=False,
)

    event_handler.on_created = on_created
    event_handler.on_deleted = on_deleted
    event_handler.on_modified = on_modified
    event_handler.on_moved = on_moved

    observer = Observer()
    observer.schedule(event_handler, path="./data", recursive=False)
    
    observer.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        
if __name__ == "__main__":
    main()