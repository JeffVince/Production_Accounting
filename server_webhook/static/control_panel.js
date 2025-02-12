document.addEventListener('DOMContentLoaded', () => {
    // Handle Trigger Buttons (require input)
    const buttons = document.querySelectorAll('.task-button');
    const inputField = document.getElementById('task_input');

    buttons.forEach(button => {
        button.addEventListener('click', () => {
            const route = button.getAttribute('data-route');
            const idValue = inputField.value.trim();

            if (!idValue) {
                alert("Please enter a valid ID.");
                return;
            }

            // Visual Feedback
            button.classList.add('active');

            fetch(`/control_panel/${route}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ value: idValue })
            })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error(`Server responded with status: ${response.status}`);
                }
            })
            .then(data => {
                alert(data.message || 'Task triggered successfully!');
            })
            .catch(err => {
                alert(`Error: ${err.message}`);
            })
            .finally(() => {
                setTimeout(() => {
                    button.classList.remove('active');
                }, 2000);
            });
        });
    });

    // Handle Sync Buttons (no input required)
    const syncButtons = document.querySelectorAll('.sync-button');
    syncButtons.forEach(button => {
        // Skip adding listener if button is marked as disabled (placeholder)
        if (button.classList.contains('disabled')) return;

        button.addEventListener('click', () => {
            const route = button.getAttribute('data-route');

            // Visual Feedback
            button.classList.add('active');

            fetch(`/sync/${route}`, {
                method: 'POST'
            })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error(`Server responded with status: ${response.status}`);
                }
            })
            .then(data => {
                alert(data.message || 'Sync task triggered successfully!');
            })
            .catch(err => {
                alert(`Error: ${err.message}`);
            })
            .finally(() => {
                setTimeout(() => {
                    button.classList.remove('active');
                }, 2000);
            });
        });
    });
});