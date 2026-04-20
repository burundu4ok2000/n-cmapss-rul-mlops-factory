import os
import sys

# Silent potential TF warnings
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 

def main():
    event_file = 'events.tfevents'
    if len(sys.argv) > 1:
        event_file = sys.argv[1]
    
    if not os.path.exists(event_file):
        print(f"Error: {event_file} not found.")
        sys.exit(1)

    print(f"--- Bayesian Telemetry Inspection (V12.2.13) ---")
    print(f"Analyzing: {event_file}\n")

    try:
        from tensorboard.backend.event_processing.event_accumulator import EventAccumulator
        
        # Load only scalars to save memory/time
        ea = EventAccumulator(event_file, size_guidance={'scalars': 100})
        ea.Reload()
        
        tags = ea.Tags().get('scalars', [])
        if not tags:
            print("No scalar tags found in the log yet. (Is the epoch finished?)")
            return

        print(f"Available tags: {', '.join(tags)}\n")

        for tag in ['mse/train', 'elbo/train', 'kl/train', 'likelihood/train']:
            if tag in tags:
                print(f"Last 5 steps for [{tag}]:")
                for e in ea.Scalars(tag)[-5:]:
                    print(f"  Step {e.step:4d}: {e.value:10.4f}")
                print()
                
    except ImportError:
        print("Error: 'tensorboard' library not found in this environment.")
        print("Please install it via: pip install tensorboard")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
