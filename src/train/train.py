import os
import sys
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import tensorflow as tf
from pathlib import Path
import matplotlib.pyplot as plt
from keras import optimizers
from src.models.loss_functions import weibullnll_loss
from src.models.mdn_model import build_mdn_model
from src.utils.plots import plot_loss

tf.keras.backend.set_floatx('float32')
pol = tf.keras.mixed_precision.Policy("float32")
tf.keras.mixed_precision.set_global_policy(pol)

#global parameters
N_COMP = 4
INIT_LR = 0.00003 
END_LR = 0.0000003 
DECAY_STEPS = 3e6
EPOCHS = 1000
BATCH_SIZE = 64
VAL_SPLIT = 0.22
SD_FACTOR = 5



if len(sys.argv) < 3:
    print("Usage: py train.py <service> <data_file>")
    sys.exit(1)

service = sys.argv[1]
data_file = sys.argv[2]
proc_time_col = f"proc_time_{service}"
delay_col = f"delay_{service}"


ROOT = Path(__file__).resolve().parents[2]  
data_path = ROOT / "data" / "dataset" / data_file
plot_path = ROOT / "plots" / "loss_plots" / f"loss_{service}" 
df = pd.read_csv(data_path, index_col=None)

if proc_time_col in df.columns and delay_col not in df.columns:
    df[proc_time_col] = (df[proc_time_col]) / 1e3 # entry point service, no delay
elif proc_time_col not in df.columns or delay_col not in df.columns:
    raise ValueError(f"No colomnus found for: '{service}'") 
else:
    df[proc_time_col] = (df[proc_time_col] + df[delay_col]) / 1e3
    
df = df[['rps', 'rps_eff', 'rep', proc_time_col]] # keep only relevant columns

df_test = []

# creation of test set
for k in range(1, 6):         
    rps_tot = np.arange(1, 30*k, k) / (SD_FACTOR*4)        
    print(f"\nReplica {k}: Generating rps from {rps_tot[:10]} / {rps_tot[10:20]} / {rps_tot[20:30]}")
    
    for i, segment in enumerate([rps_tot[:10], rps_tot[10:20], rps_tot[20:30]]):
        if len(segment) == 0:
            print(f"  Segment {i} empty! Skipping.")
            continue

        rps_test = np.random.choice(segment, 1)[0]
        available_rps = df[df.rep == k]['rps'].unique()

        if len(available_rps) == 0:
            print(f"  No RPS available for replica {k}")
            continue

        closest_rps = available_rps[np.abs(available_rps - rps_test).argmin()] # find the closest RPS in the available ones
        subset = df[(df.rep == k) & (df.rps == closest_rps)]
        print(f"  Trying closest rps = {closest_rps:.2f} for rep = {k} ⇒ Found {len(subset)} rows.")
        
        if not subset.empty:
            df_test.append(subset)
            df.drop(subset.index, inplace=True)

df_test = pd.concat(df_test, axis=0, ignore_index=True)
df_train = df.sample(frac=1)

print("Test RPS per replica:")
for rep in range(1, 6):
    print(f"Replica {rep}:", df_test[df_test.rep == rep].rps.unique())


target = df_train[proc_time_col]
features = df_train[['rps_eff', 'rep']]

model, norm_layer = build_mdn_model(input_shape=(2,), n_components=N_COMP)


loss = weibullnll_loss(model.get_layer("y_real").output, 
                       model.get_layer("alpha").output,
                       model.get_layer("concentration").output,
                       model.get_layer("scale").output)

model.add_loss(loss)

# learning rate schedule
lr_schedule = tf.keras.optimizers.schedules.PolynomialDecay(
    initial_learning_rate=INIT_LR,
    end_learning_rate=END_LR,
    decay_steps=DECAY_STEPS
)

adamOptimizer = optimizers.Adam(learning_rate=lr_schedule) 

model.summary()
model.compile(optimizer=adamOptimizer)

norm_layer.adapt(features)
norm_layer.get_weights()


model_dir = ROOT / "saved_models" / f"{service}"
checkpoint_dir = model_dir / "checkpoint"
checkpoint_dir.mkdir(parents=True, exist_ok=True)

checkpoint_cb = tf.keras.callbacks.ModelCheckpoint(
    filepath=checkpoint_dir / "ckpt",
    save_weights_only=True,
    monitor='val_loss',
    mode='min',
    save_best_only=True
)


history = model.fit([features, target],
                    epochs=EPOCHS,
                    batch_size=BATCH_SIZE,
                    validation_split=VAL_SPLIT,
                    callbacks=[checkpoint_cb],
                    verbose=1)

print(f"Final training loss: {history.history['loss'][-1]:.4f}")

model.save(model_dir / "model")
df_train.to_csv(model_dir / f"{service}_train.csv", index=False)
df_test.to_csv(model_dir / f"{service}_test.csv", index=False)

plot_loss(history, plot_path)
