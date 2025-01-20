// control_panel.js

document.addEventListener('DOMContentLoaded', () => {
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

            // Visual Feedback: Add 'active' class to light up the button
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
                // Remove 'active' class after 2 seconds
                setTimeout(() => {
                    button.classList.remove('active');
                }, 2000);
            });
        });
    });
});