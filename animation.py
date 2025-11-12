import os
import imageio
from graphviz import Digraph
import numpy as np
# --- 1. Define the Star Schema Structure ---

# Central Fact Table
fact_table = "fact_repairs"

# Dimension Tables (in the order you want them to appear)
dimensions = {
    "dim_date": "Date & Time Context",
    "dim_garage": "Garage & Location",
    "dim_vehicle": "Vehicle Details",
    "dim_customer": "Customer Information",
    "dim_technician": "Technician Details",
    "dim_repair_details": "Repair Specifics",
    "dim_job_context": "Job Context (Weather, etc.)"
}

# --- 2. Animation Configuration ---
output_gif_path = 'star_schema_animation.gif'
frames_dir = 'animation_frames'
frame_rate = 1.2  # Seconds per frame

# --- 3. Generate the Animation Frames ---

# --- 3. Generate the Animation Frames ---

print(f"Creating frames in directory: {frames_dir}")
os.makedirs(frames_dir, exist_ok=True)
frame_files = []

# --- Frame 0: The Central Fact Table Appears ---
dot = Digraph('StarSchema', graph_attr={'bgcolor': 'transparent'})
dot.node(fact_table, style='filled', fillcolor='#ffcccc', shape='oval') # Make fact table stand out
frame_base_name = os.path.join(frames_dir, 'frame_00') # Specify base name without .png
dot.render(frame_base_name, format='png', cleanup=True) # Let format handle the .png
frame_path = frame_base_name + '.png' # Manually add .png to get the actual file path
frame_files.append(frame_path)
print("  - Generated frame 0: Fact Table")

# --- Loop to add each dimension one by one ---
for i, (dim_table, label) in enumerate(dimensions.items()):
    # Add the new dimension node
    dot.node(dim_table, style='filled', fillcolor='#cce5ff', shape='box')
    
    # Add the edge connecting the dimension to the fact table
    dot.edge(dim_table, fact_table)
    
    # Render and save the frame
    frame_number = i + 1
    frame_base_name = os.path.join(frames_dir, f'frame_{frame_number:02d}') # Specify base name
    dot.render(frame_base_name, format='png', cleanup=True) # Let format handle the .png
    frame_path = frame_base_name + '.png' # Manually add .png to get the actual file path
    frame_files.append(frame_path)
    print(f"  - Generated frame {frame_number}: Added {dim_table}")
    # --- 4. Create the Animated GIF ---
import imageio.v3 as iio

output_video_path = 'star_schema_animation.mp4'


import imageio
import numpy as np # Often useful for image manipulation

# ... (your existing code for frame_files and other variables) ...

# --- Add this diagnostic block ---
print("Checking image dimensions:")
first_image_shape = None
for i, frame_file in enumerate(frame_files):
    try:
        image = imageio.imread(frame_file)
        current_shape = image.shape
        print(f"  Frame {i+1}: {frame_file} - Shape: {current_shape}")

        if first_image_shape is None:
            first_image_shape = current_shape
        elif current_shape != first_image_shape:
            print(f"    ⚠️ Mismatch found! Expected {first_image_shape}, got {current_shape}")
            # You might want to break here or log more details
    except Exception as e:
        print(f"  Error reading {frame_file}: {e}")
print("Finished dimension check.")
# --- End diagnostic block ---

output_video_path = 'star_schema_animation.mp4'
print(f"Creating MP4 video: {output_video_path}")
with imageio.get_writer(
    output_video_path,
    fps=1 / frame_rate,
    codec='libx264',
    quality=8
) as writer:
    for frame_file in frame_files:
        image = imageio.imread(frame_file)
        writer.append_data(image)

print("✅ Video creation complete!")