import uuid
from flask import Flask, request, jsonify, render_template, Blueprint
from server_agent.agent_app import ChatAgent

agent_bp = Blueprint("agent_bp", __name__)

# In-memory storage for chat sessions.
# Instead of storing just a list of messages, we now store a dedicated ChatAgent
# instance per chat to maintain its own conversation history.
CHAT_THREADS = {}

@agent_bp.route('/index')
def index():
    """Serves the main chat page (index.html)."""
    return render_template('index.html')

@agent_bp.route('/api/chats', methods=['GET'])
def list_chats():
    """
    Returns a list of all existing chats (IDs and titles).
    Example response: [{ 'chat_id': 'abc123', 'title': 'Chat with Agent' }, ...]
    """
    chat_list = []
    for chat_id, chat_data in CHAT_THREADS.items():
        chat_list.append({
            'chat_id': chat_id,
            'title': chat_data['title']
        })
    return jsonify(chat_list), 200

@agent_bp.route('/api/chats', methods=['POST'])
def create_chat():
    """
    Creates a new chat and returns its ID and default title.
    Request body can optionally include 'title'.
    """
    data = request.get_json() or {}
    title = data.get('title', 'New Chat')
    chat_id = str(uuid.uuid4())[:8]  # shortened random ID

    # Create a new ChatAgent instance for this chat session.
    agent_instance = ChatAgent(model="gpt-3.5-turbo")
    CHAT_THREADS[chat_id] = {
        'title': title,
        'agent': agent_instance
    }
    return jsonify({
        'chat_id': chat_id,
        'title': title
    }), 201

@agent_bp.route('/api/chats/<chat_id>/messages', methods=['GET'])
def get_chat_messages(chat_id):
    """
    Return all messages in a given chat.
    The conversation history is retrieved from the ChatAgent instance.
    """
    if chat_id not in CHAT_THREADS:
        return jsonify({'error': 'Chat not found'}), 404

    agent_instance = CHAT_THREADS[chat_id]['agent']
    # Optionally remove the system prompt (the first message) from the history.
    messages = agent_instance.conversation_history[1:] if len(agent_instance.conversation_history) > 1 else []
    return jsonify({
        'chat_id': chat_id,
        'title': CHAT_THREADS[chat_id]['title'],
        'messages': messages
    }), 200

@agent_bp.route('/api/chats/<chat_id>/message', methods=['POST'])
def post_message(chat_id):
    """
    Send a message to a specific chat session.
    The request body should have { "message": "..." }.
    The ChatAgent instance processes the new message (appending it to its internal conversation
    and generating a response), and the assistant's reply is returned to the client.
    """
    if chat_id not in CHAT_THREADS:
        return jsonify({'error': 'Chat not found'}), 404

    data = request.get_json() or {}
    user_msg = data.get('message', '').strip()
    if not user_msg:
        return jsonify({'error': 'No message provided'}), 400

    agent_instance = CHAT_THREADS[chat_id]['agent']
    # Call the new handle_user_input method which manages conversation history internally.
    agent_reply = agent_instance.handle_user_input(user_msg)

    return jsonify({'reply': agent_reply}), 200

@agent_bp.route('/api/chats/<chat_id>', methods=['DELETE'])
def delete_chat(chat_id):
    """
    Delete an entire chat by its ID.
    """
    if chat_id in CHAT_THREADS:
        del CHAT_THREADS[chat_id]
        return jsonify({'deleted': chat_id}), 200
    else:
        return jsonify({'error': 'Chat not found'}), 404