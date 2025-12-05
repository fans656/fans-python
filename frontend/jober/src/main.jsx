import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter as Router } from 'react-router-dom';

import 'src/style.css'
import App from 'src/app'

createRoot(document.getElementById('root')).render(
  <Router>
    <App/>
  </Router>
)
