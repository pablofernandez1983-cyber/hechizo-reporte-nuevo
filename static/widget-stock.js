(function () {
  const ENDPOINT = 'https://hechizo-reporte-nuevo-production.up.railway.app/notify';
  const STORE_ID = (window.LS && window.LS.store && window.LS.store.id) ? window.LS.store.id : 0;

  const style = document.createElement('style');
  style.textContent = `
    #sn-container { margin-top: 1rem; }
    #sn-btn {
      width: 100%; padding: 10px;
      border: 1px solid #6c757d; color: #6c757d;
      background: white; cursor: pointer;
      text-align: center; font-size: 14px;
    }
    #sn-btn:hover { background: #f8f9fa; }
    #sn-overlay {
      display: none; position: fixed;
      top: 0; left: 0; width: 100%; height: 100%;
      background: rgba(0,0,0,0.5); z-index: 9998;
    }
    #sn-modal {
      display: none; position: fixed;
      top: 50%; left: 50%;
      transform: translate(-50%, -50%);
      background: white; padding: 24px;
      z-index: 9999; width: 90%; max-width: 400px;
      border-radius: 4px;
    }
    #sn-close { float: right; cursor: pointer; font-size: 20px; color: #999; }
    #sn-title { font-size: 15px; margin: 16px 0 12px; clear: both; }
    #sn-product-name { font-weight: bold; font-size: 13px; margin-bottom: 12px; color: #444; }
    #sn-email {
      width: 100%; padding: 8px; border: 1px solid #ccc;
      margin-bottom: 12px; font-size: 14px; box-sizing: border-box;
    }
    #sn-submit {
      width: 100%; padding: 10px; background: #333;
      color: white; border: none; cursor: pointer; font-size: 14px;
    }
    #sn-submit:hover { background: #555; }
    #sn-success { display: none; text-align: center; padding: 16px 0; color: #555; }
    #sn-error { color: red; font-size: 12px; margin-bottom: 8px; display: none; }
    #sn-powered { text-align: center; font-size: 11px; color: #ccc; margin-top: 12px; }
  `;
  document.head.appendChild(style);

  function getProductData() {
    const productId = (window.LS && window.LS.product && window.LS.product.id)
      ? String(window.LS.product.id)
      : null;

    const variantEl = document.querySelector('.js-variation-option.selected, .js-variation-option[data-selected], input[name="id"]');
    const variantId = variantEl
      ? (variantEl.getAttribute('data-id') || variantEl.getAttribute('data-variation-id') || variantEl.value || productId)
      : productId;

    const nameEl = document.querySelector('h1.product-name, h1, .product-name');
    const productName = nameEl ? nameEl.textContent.trim() : '';

    const variantNameEl = document.querySelector('.js-variation-option.selected, .js-variation-option[data-selected]');
    const variantName = variantNameEl ? variantNameEl.textContent.trim() : '';

    const skuEl = document.querySelector('[data-sku], [itemprop="sku"], .product-sku, .js-sku');
    const sku = skuEl ? (skuEl.getAttribute('data-sku') || skuEl.getAttribute('content') || skuEl.textContent).trim() : '';

    return { productId, variantId, productName, variantName, sku };
  }

  function isOutOfStock() {
    const btn = document.querySelector('.js-addtocart.nostock, .js-addtocart[disabled]');
    return btn !== null;
  }

  function buildWidget() {
    const container = document.createElement('div');
    container.id = 'sn-container';
    container.innerHTML = `
      <button id="sn-btn">🔔 Avisame cuando haya stock</button>
      <div id="sn-overlay"></div>
      <div id="sn-modal">
        <span id="sn-close">×</span>
        <div id="sn-title">Dejanos tu email y te avisamos cuando el producto vuelva a estar disponible:</div>
        <div id="sn-product-name"></div>
        <div id="sn-error">Por favor ingresá un email válido.</div>
        <input id="sn-email" type="email" placeholder="tucorreo@email.com" />
        <button id="sn-submit">Avisame</button>
        <div id="sn-success">✅ ¡Te avisamos cuando vuelva!</div>
        <div id="sn-powered">Notificaciones de stock</div>
      </div>
    `;
    return container;
  }

  function inject() {
    if (!isOutOfStock()) return;

    const { productId, variantId, productName, variantName, sku } = getProductData();
    if (!productId) return;

    const anchor = document.querySelector('form.js-product-form');
    if (!anchor) return;

    const widget = buildWidget();
    anchor.parentNode.insertBefore(widget, anchor.nextSibling);

    document.getElementById('sn-product-name').textContent = productName;

    const btn     = document.getElementById('sn-btn');
    const overlay = document.getElementById('sn-overlay');
    const modal   = document.getElementById('sn-modal');
    const close   = document.getElementById('sn-close');
    const submit  = document.getElementById('sn-submit');
    const emailIn = document.getElementById('sn-email');
    const success = document.getElementById('sn-success');
    const error   = document.getElementById('sn-error');

    function openModal() {
      overlay.style.display = 'block';
      modal.style.display = 'block';
      emailIn.focus();
    }
    function closeModal() {
      overlay.style.display = 'none';
      modal.style.display = 'none';
    }

    btn.addEventListener('click', openModal);
    overlay.addEventListener('click', closeModal);
    close.addEventListener('click', closeModal);

    submit.addEventListener('click', async function () {
      const email = emailIn.value.trim();
      error.style.display = 'none';

      if (!email || !email.includes('@')) {
        error.style.display = 'block';
        return;
      }

      submit.disabled = true;
      submit.textContent = 'Enviando...';

      try {
        const res = await fetch(ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            email,
            product_id:   parseInt(productId),
            variant_id:   parseInt(variantId),
            store_id:     STORE_ID,
            product_name: productName,
            variant_name: variantName,
            sku:          sku || undefined,
          })
        });

        if (res.ok) {
          emailIn.style.display = 'none';
          submit.style.display = 'none';
          success.style.display = 'block';
        } else {
          error.textContent = 'Hubo un error, intentá de nuevo.';
          error.style.display = 'block';
          submit.disabled = false;
          submit.textContent = 'Avisame';
        }
      } catch (e) {
        error.textContent = 'Hubo un error, intentá de nuevo.';
        error.style.display = 'block';
        submit.disabled = false;
        submit.textContent = 'Avisame';
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }

})();
