import torch
import torch.nn as nn
import torch.nn.functional as F

class AdvancedChurnPredictor(nn.Module):
    def __init__(self, input_dim, hidden_dims=[64, 32], dropout_rate=0.3):
        super(AdvancedChurnPredictor, self).__init__()
        self.batch_norm = nn.BatchNorm1d(input_dim)
        self.layers = nn.ModuleList()
        
        all_dims = [input_dim] + hidden_dims
        for i in range(len(all_dims) - 1):
            self.layers.append(nn.Linear(all_dims[i], all_dims[i+1]))
            self.layers.append(nn.ReLU())
            self.layers.append(nn.Dropout(dropout_rate))
        
        self.output = nn.Linear(hidden_dims[-1], 1)
        
    def forward(self, x):
        x = self.batch_norm(x)
        for layer in self.layers:
            x = layer(x)
        return torch.sigmoid(self.output(x))