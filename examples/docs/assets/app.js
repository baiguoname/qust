
const search = document.querySelector('#search');
if (search) {
  search.addEventListener('input', () => {
    const q = search.value.trim().toLowerCase();
    document.querySelectorAll('.sidebar a,.op-list li').forEach((el) => {
      el.style.display = !q || el.textContent.toLowerCase().includes(q) ? '' : 'none';
    });
  });
}
