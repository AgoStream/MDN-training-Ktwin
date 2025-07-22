from matplotlib import pyplot as plt

def plot_loss(history, path, zoom=False):
    plt.plot(history.history['loss'], label='loss')
    plt.plot(history.history['val_loss'], label='val_loss')
    plt.xlabel('Epoch')
    plt.ylabel('NLL - Negative Log-Likelihood')
    if zoom:
        plt.ylim(min(history.history['loss']), min(history.history['loss']) + 0.03)
    plt.legend()
    plt.grid(True)
    plt.savefig(f"{path}")
