#!/usr/bin/env python3
"""
Script to generate a test PyTorch model for the LocalModelIntentLayer.
This creates a dummy trained model with proper vocabulary and intent labels.
"""

import sys
import torch
import torch.nn as nn
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))


def create_test_model():
    """Create a test model with some dummy training."""
    
    # Import here to avoid circular imports
    from intent_classifier.intent_layers.local_model import IntentClassificationModel
    
    # Create vocabulary (same as in the class)
    vocab = {
        '<PAD>': 0, '<UNK>': 1, '<START>': 2, '<END>': 3,
        'hello': 4, 'hi': 5, 'goodbye': 6, 'bye': 7, 'thanks': 8, 'thank': 9,
        'you': 10, 'please': 11, 'help': 12, 'what': 13, 'when': 14, 'where': 15,
        'how': 16, 'why': 17, 'who': 18, 'can': 19, 'could': 20, 'would': 21,
        'is': 22, 'are': 23, 'was': 24, 'were': 25, 'the': 26, 'a': 27, 'an': 28,
        'and': 29, 'or': 30, 'but': 31, 'in': 32, 'on': 33, 'at': 34, 'to': 35,
        'for': 36, 'with': 37, 'by': 38, 'from': 39, 'up': 40, 'down': 41,
    }
    
    # Create intent labels
    intent_labels = ['greeting', 'goodbye', 'question', 'request', 'thanks', 'complaint', 'compliment', 'unknown']
    
    # Initialize model
    model = IntentClassificationModel(
        vocab_size=len(vocab),
        embedding_dim=128,
        hidden_dim=256,
        num_classes=len(intent_labels)
    )
    
    # Create some dummy training data to give the model reasonable weights
    print("Training dummy model...")
    model.train()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    loss_fn = nn.CrossEntropyLoss()
    
    # Create some sample training data
    sample_inputs = [
        ([vocab.get(w, vocab['<UNK>']) for w in ['hello', 'how', 'are', 'you']] + [vocab['<PAD>']] * 28, 0),  # greeting
        ([vocab.get(w, vocab['<UNK>']) for w in ['goodbye', 'see', 'you', 'later']] + [vocab['<PAD>']] * 28, 1),  # goodbye
        ([vocab.get(w, vocab['<UNK>']) for w in ['what', 'is', 'the', 'weather']] + [vocab['<PAD>']] * 28, 2),  # question
        ([vocab.get(w, vocab['<UNK>']) for w in ['please', 'help', 'me', 'with']] + [vocab['<PAD>']] * 28, 3),  # request
        ([vocab.get(w, vocab['<UNK>']) for w in ['thank', 'you', 'very', 'much']] + [vocab['<PAD>']] * 28, 4),  # thanks
    ]
    
    # Simple training loop
    for epoch in range(50):
        total_loss = 0
        for inputs, target in sample_inputs:
            optimizer.zero_grad()
            
            input_tensor = torch.tensor([inputs], dtype=torch.long)
            target_tensor = torch.tensor([target], dtype=torch.long)
            
            logits, _ = model(input_tensor)
            loss = loss_fn(logits, target_tensor)
            
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
        
        if epoch % 10 == 0:
            print(f"Epoch {epoch}: Loss = {total_loss:.4f}")
    
    model.eval()
    
    # Create the checkpoint
    checkpoint = {
        'model_state_dict': model.state_dict(),
        'vocab': vocab,
        'intent_labels': intent_labels,
        'model_config': {
            'vocab_size': len(vocab),
            'embedding_dim': 128,
            'hidden_dim': 256,
            'num_classes': len(intent_labels)
        }
    }
    
    # Ensure the results directory exists
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(exist_ok=True)
    
    # Save the model
    model_path = results_dir / "latest.pt"
    torch.save(checkpoint, model_path)
    
    print(f"Test model saved to: {model_path}")
    print(f"Vocabulary size: {len(vocab)}")
    print(f"Intent labels: {intent_labels}")
    
    return model_path


if __name__ == "__main__":
    create_test_model()
