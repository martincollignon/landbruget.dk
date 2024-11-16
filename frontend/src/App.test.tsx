import React from 'react';
import { render } from '@testing-library/react';
import App from './App';

test('renders map container', () => {
  render(<App />);
  const mapElement = document.querySelector('.map-container');
  expect(mapElement).toBeInTheDocument();
});
