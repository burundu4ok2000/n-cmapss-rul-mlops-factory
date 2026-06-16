"""
CORE DOMAIN: Bayesian Architecture Definitions (V0.2.1)
ROLE: Implementation of the BigCeption manifold for high-fidelity RUL inference.
STATUS: PRODUCTION_READY

CONTRACT:
    - IN: [Batch, Window, Features] Normalized telemetry tensors.
    - OUT: [Batch, Out_Size] Probabilistic RUL predictions (Mean, Uncertainty).

SYSTEM-LEVEL INVARIANTS:
    - WIN_LENGTH_SYNC: Must be 30 cycles to match training receptive field.
    - FEATURE_COUNT_SYNC: Must be 18 to match N-CMAPSS (DS02-006) protocol.
    - SOFTPLUS_GUARD: Mandatory non-negativity constraint on the likelihood head.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F

# --- 1. ARCHITECTURAL BUILDING BLOCKS (V0.2.0) ---

class InceptionModule(nn.Module):
    """
    Core building block implementing multi-scale receptive fields.
    
    Orchestrates parallel convolutional branches (1x1, 3x3, 5x5) and 
    a max-pooling branch to capture cross-scale temporal features.
    """
    def __init__(
        self, 
        n_features,
        filter1x1, 
        filter3x3, 
        filter5x5, 
        filterpool,
        activation: nn.Module,
        bias=True,
        dropout=0,
    ):
        super().__init__()
        self.conv1 = nn.Sequential(
            nn.Conv1d(n_features, filter1x1, kernel_size=1, padding='same', bias=bias),
            activation()
        )
        self.conv3 = nn.Sequential(
            nn.Conv1d(n_features, filter3x3, kernel_size=3, padding='same', bias=bias),
            activation()
        )
        self.conv5 = nn.Sequential(
            nn.Conv1d(n_features, filter5x5, kernel_size=5, padding='same', bias=bias),
            activation()
        )
        self.convpool = nn.Sequential(
            nn.MaxPool1d(kernel_size=3, stride=1, padding=1),
            nn.Conv1d(n_features, filterpool, kernel_size=3, padding='same', bias=bias),
            activation()
        )
        if dropout > 0:
            self.conv1.add_module('branch1_dropout', nn.Dropout(dropout/4))
            self.conv3.add_module('branch2_dropout', nn.Dropout(dropout/4))
            self.conv5.add_module('branch3_dropout', nn.Dropout(dropout/4))
            self.convpool.add_module('branch4_dropout', nn.Dropout(dropout/4))
            
    def forward(self, x):
        """
        Executes parallel feature extraction across 4 branches.
        
        Args:
            x (torch.Tensor): [Batch, Features, Window]
        
        Returns:
            torch.Tensor: Concatenated features from all scales.
        """
        branches = [self.conv1(x), self.conv3(x), self.conv5(x), self.convpool(x)]
        return torch.cat(branches, 1)


class InceptionModuleReducDim(nn.Module):
    """
    Dimension-reduced Inception module for deeper hierarchical abstraction.
    
    Implements 1x1 bottleneck convolutions to optimize computational 
    efficiency before high-dimensional spatial convolutions.
    """
    def __init__(
        self, 
        n_features,
        filter1x1, 
        reduc3x3,
        filter3x3, 
        reduc5x5,
        filter5x5, 
        filterpool,
        activation: nn.Module,
        bias=True,
        dropout=0,
    ):
        super().__init__()
        self.branch1 = nn.Sequential(
            nn.Conv1d(n_features, filter1x1, kernel_size=1, padding='same', bias=bias),
            activation()
        )
        self.branch2 = nn.Sequential(
            nn.Conv1d(n_features, reduc3x3, kernel_size=1, padding=0, bias=bias),
            activation(),
            nn.Conv1d(reduc3x3, filter3x3, kernel_size=3, padding='same', bias=bias),
            activation()
        )
        self.branch3 = nn.Sequential(
            nn.Conv1d(n_features, reduc5x5, kernel_size=1, padding=0, bias=bias),
            activation(),
            nn.Conv1d(reduc5x5, filter5x5, kernel_size=5, padding='same', bias=bias), 
            activation()
        )
        self.branch4 = nn.Sequential(
            nn.MaxPool1d(kernel_size=3, stride=1, padding=1),
            nn.Conv1d(n_features, filterpool, kernel_size=1, padding=0, bias=bias),
            activation()
        )
        if dropout > 0:
            self.branch1.add_module('branch1_dropout', nn.Dropout(dropout/4))
            self.branch2.add_module('branch2_dropout', nn.Dropout(dropout/4))
            self.branch3.add_module('branch3_dropout', nn.Dropout(dropout/4))
            self.branch4.add_module('branch4_dropout', nn.Dropout(dropout/4))
    
    def forward(self, x):
        """
        Executes forward bottleneck pass.
        
        Returns:
            torch.Tensor: Concatenated multi-scale activations.
        """
        branches = [self.branch1(x), self.branch2(x), self.branch3(x), self.branch4(x)]
        return torch.cat(branches, 1)

# --- 2. GLOBAL ARCHITECTURE: BIGCEPTION (V0.2.1) ---

class BigCeption(nn.Module):
    """
    Operational Predictive Network for N-CMAPSS Diagnostics.
    
    Synchronized with the DS02-006 mission specification. Implements a 
    dual-inception backbone followed by a dense decision manifold.
    """
    def __init__(
        self, 
        win_length=30, 
        n_features=18, 
        activation='leaky_relu', 
        bias=True,
        dropout=0, 
        out_size=2
    ):
        super().__init__()
        
        # [INVARIANT:Fidelity] Receptive field synchronization check.
        assert n_features == 18, "Mission Violation: Model expects 18-feature telemetry."

        # Dynamic Activation Mapping
        if activation == 'relu':
            act = nn.ReLU
        elif activation == 'leaky_relu':
            act = nn.LeakyReLU
        else:
            raise ValueError(f"[ERROR:Architecture] Unsupported activation: {activation}")

        self.out_size = out_size

        # Core Feature Extraction Backbone
        self.layers = nn.Sequential(
            InceptionModule(n_features, 27, 27, 27, 27, activation=act, 
                          dropout=dropout, bias=bias),
            InceptionModuleReducDim(27+27+27+27, 16, 64, 16, 64, 16, 32, 
                                   dropout=dropout, activation=act, bias=bias),
            nn.Flatten(),
            nn.Linear((16+16+16+32) * win_length, 64),
            act(), 
        )
        if dropout > 0:
            self.layers.add_module('dense_dropout', nn.Dropout(dropout))

        # Prediction Head: Probability Manifold (RUL Mean + LogVar mapping)
        self.last = nn.Linear(64, self.out_size)
        self.thresh = nn.Threshold(1e-9, 1e-9)
        
    def forward(self, x):
        """
        Transforms windowed telemetry into Gaussian likelihood parameters.
        
        Args:
            x (torch.Tensor): [Batch, Window, Features] (e.g., [1, 30, 18])
        
        Returns:
            torch.Tensor: [Batch, 2] -> [RUL_Mean, RUL_Uncertainty] (Softplus-guarded).
        """
        # 1. Coordinate Transformation: [B, W, F] -> [B, F, W] for Conv1d
        x = x.transpose(2, 1)
        
        # 2. Hierarchical Extraction
        features = self.layers(x)
        prediction = self.last(features)
        
        # 3. Numerical Stability Barrier (V18.4)
        # Ensure positive-only manifold for physical time (RUL) and Variance.
        output = F.softplus(prediction)
        output = self.thresh(output)
        
        return output
