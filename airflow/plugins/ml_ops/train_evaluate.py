import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import KFold
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score
import pandas as pd
import numpy as np
from ml_ops.model import AdvancedChurnPredictor

def train_model(X, y, model, criterion, optimizer, epochs=100, batch_size=32):
    dataset = torch.utils.data.TensorDataset(X, y)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)
    
    for epoch in range(epochs):
        model.train()
        for batch_X, batch_y in dataloader:
            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y.unsqueeze(1))
            loss.backward()
            optimizer.step()
        
        if (epoch+1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}')
    
    return model

def evaluate_model(model, X, y):
    model.eval()
    with torch.no_grad():
        outputs = model(X)
        predictions = (outputs >= 0.5).float()
        auc = roc_auc_score(y.numpy(), outputs.numpy())
        precision = precision_score(y.numpy(), predictions.numpy())
        recall = recall_score(y.numpy(), predictions.numpy())
        f1 = f1_score(y.numpy(), predictions.numpy())
    
    return {
        'auc': auc,
        'precision': precision,
        'recall': recall,
        'f1': f1
    }

def cross_validate(X, y, n_splits=5):
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    results = []
    
    for train_index, val_index in kf.split(X):
        X_train, X_val = X[train_index], X[val_index]
        y_train, y_val = y[train_index], y[val_index]
        
        model = AdvancedChurnPredictor(X.shape[1])
        criterion = nn.BCELoss()
        optimizer = optim.Adam(model.parameters())
        
        trained_model = train_model(X_train, y_train, model, criterion, optimizer)
        results.append(evaluate_model(trained_model, X_val, y_val))
    
    return pd.DataFrame(results).mean().to_dict()

def train_final_model(X, y):
    model = AdvancedChurnPredictor(X.shape[1])
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters())
    
    return train_model(X, y, model, criterion, optimizer)

def save_model(model, path):
    torch.save(model.state_dict(), path)

def load_model(path, input_dim):
    model = AdvancedChurnPredictor(input_dim)
    model.load_state_dict(torch.load(path))
    return model