from sentence_transformers import SentenceTransformer
import pickle

model = SentenceTransformer('all-MiniLM-L6-v2', device='cuda')

# Transformer models like BERT / RoBERTa / DistilBERT etc. the runtime and the memory requirement grows quadratic with the input length. This limits transformers to inputs of certain lengths. A common value for BERT & Co. are 512 word pieces, which corresponde to about 300-400 words (for English). Longer texts than this are truncated to the first x word pieces.

# By default, the provided methods use a limit fo 128 word pieces, longer inputs will be truncated. You can get and set the maximal sequence length like this:
print("Max Sequence Length:", model.max_seq_length)

# Change the length to 200
model.max_seq_length = 512

print("Max Sequence Length:", model.max_seq_length)

# Our sentences we like to encode
sentences = ['This framework generates embeddings for each input sentence',
             'Sentences are passed as a list of string.',
             'The quick brown fox jumps over the lazy dog.']

# Sentences are encoded by calling model.encode()
embeddings = model.encode(sentences)

# Store sentences & embeddings on disc
with open('embeddings.pkl', "wb") as fOut:
    pickle.dump({'sentences': sentences, 'embeddings': embeddings},
                fOut, protocol=pickle.HIGHEST_PROTOCOL)

# Load sentences & embeddings from disc
with open('embeddings.pkl', "rb") as fIn:
    stored_data = pickle.load(fIn)
    stored_sentences = stored_data['sentences']
    stored_embeddings = stored_data['embeddings']

# Print the embeddings
for sentence, embedding in zip(stored_sentences, stored_embeddings):
    print("Sentence:", sentence)
    print(len(embedding))
    print("Embedding:", embedding)
    print("")
