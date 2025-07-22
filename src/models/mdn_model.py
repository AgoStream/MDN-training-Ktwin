import tensorflow as tf
from tensorflow.keras import Input, Model
from tensorflow.keras.layers import Dense, LeakyReLU, BatchNormalization, Normalization
from src.models.loss_functions import weibullnll_loss

def build_mdn_model(n_components=4, input_shape=(2,),
                    initializer='he_normal', loss_type='weibull'):
    """
    Crea un MDN parametrico per input (rps_eff, rep) e target generico.

    Args:
        n_components (int): Numero di componenti nella mixture.
        input_shape (tuple): Forma degli input (default 2: rps_eff e rep).
        initializer (str): Metodo inizializzazione pesi.
        loss_type (str): Tipo di loss ('weibull', 'gamma', 'gaussian').

    Returns:
        model: Un oggetto keras.Model compilato.
        norm_layer: Layer Normalization adattabile esternamente.
    """

    InputLayer = Input(shape=(2,), name="input_1")  # 2 input neurons (rps, rep)

    Norm_layer = Normalization()    # Normalize the input for faster convergence

    # First layer:
    Layer_1 = Dense(10, kernel_initializer=initializer)(Norm_layer(InputLayer))
    Layer_1 = BatchNormalization()(Layer_1)         # Add normalization of the input of the activation func. for faster convergence
    Layer_1 = LeakyReLU()(Layer_1)

    # Second Layer:
    Layer_2 = Dense(10, kernel_initializer=initializer)(Layer_1)
    Layer_2 = BatchNormalization()(Layer_2)
    Layer_2 = LeakyReLU()(Layer_2)

    # Define outputs
    alpha = Dense(n_components, activation="softmax", kernel_initializer=initializer, name="alpha")(Layer_2)
    concentration = Dense(n_components, activation=lambda x: tf.nn.softplus(x) +  1e-8, kernel_initializer=initializer, name="concentration")(Layer_2)       # + 1e-8 avoid too small inputs
    scale = Dense(n_components, activation=lambda x: tf.nn.softplus(x) +  1e-8, kernel_initializer=initializer, name="scale")(Layer_2)

    # Define input for the real values (used only during training to compute the Negative Log-likelihood)
    y_real = Input(shape=(1,), name="y_real")

    # Define the loss
    lossF = weibullnll_loss(y_real, alpha, concentration, scale)  # gammanll_loss(y_real, alpha, mu, sigma) 

    # Instantiate the model
    mdn_model = Model(inputs=[InputLayer, y_real], outputs=[alpha, concentration, scale])
    mdn_model.add_loss(lossF)

    return mdn_model, Norm_layer